#include "crpc/base/error_code.h"
#include "crpc/base/log.h"
#include "crpc/base/msg_req.h"
#include "crpc/base/run_time.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_pool.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/rpc/crpc_async_channel.h"
#include "crpc/rpc/crpc_channel.h"
#include "crpc/rpc/crpc_controller.h"

#include <memory>
#include <string>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace crpc {

namespace {

/**
 * 全局共享的异步执行环境
 * 无论是客户端还是服务端，使用AsyncChannel时都会在、且只会在整个进程中新创建一个AsyncClientRuntime实例对象
 * AsyncClientRuntime实例对象的创建会在进程中新创建一个线程，为进程中所有的AsyncChannel同时提供服务
 */
class AsyncClientRuntime {
public:
	AsyncClientRuntime() : coroutine_pool_(16, 128 * 1024) {
		// 创建独立的IO线程为异步RPC请求提供服务
		io_thread_ = std::make_shared<IOThread>();
		io_thread_->setThreadIndex(0);
		sem_post(io_thread_->getStartSemaphore());
	}

	Coroutine::Ptr addCoroutine(std::function<void()> callback) {
		Coroutine::Ptr coroutine = coroutine_pool_.getCoroutineInstance();
		coroutine->setCallBack(callback);
		io_thread_->getReactor()->addCoroutine(coroutine, true);
		return coroutine;
	}

	void returnCoroutine(Coroutine::Ptr coroutine) { coroutine_pool_.returnCoroutine(coroutine); }

private:
	CoroutinePool coroutine_pool_;	// 为每次异步 RPC 提供工作协程
	IOThread::Ptr io_thread_;		// 提供 Reactor 事件循环
};

AsyncClientRuntime* GetAsyncClientRuntime() {
	// 进程退出时由操作系统统一回收，避免静态析构阶段先销毁 Reactor 或协程池
	static AsyncClientRuntime* runtime = new AsyncClientRuntime();
	return runtime;
}

}  // namespace

CrpcAsyncChannel::CrpcAsyncChannel(NetAddress::Ptr addr) {
	rpc_channel_ = std::make_shared<CrpcChannel>(addr);
	current_io_thread_ = IOThread::getCurrentIOThread();
	if (current_io_thread_ && !Coroutine::isMainCoroutine()) {
		current_coroutine_ = Coroutine::getCurrentCoroutine();
		use_coroutine_wait_ = true;
	}
}

CrpcAsyncChannel::~CrpcAsyncChannel() {
	if (pending_coroutine_) {
		GetAsyncClientRuntime()->returnCoroutine(pending_coroutine_);
	}
}

CrpcChannel* CrpcAsyncChannel::getRpcChannel() {
	return rpc_channel_.get();
}

void CrpcAsyncChannel::saveCallee(ControllerPtr controller, MessagePtr request, MessagePtr response,
								  ClosurePtr closure) {
	controller_ = controller;
	request_ = request;
	response_ = response;
	closure_ = closure;
	is_pre_set_ = true;
}

void CrpcAsyncChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
								  google::protobuf::RpcController* controller,
								  const google::protobuf::Message* request,
								  google::protobuf::Message* response,
								  google::protobuf::Closure* done) {
	(void)request;
	(void)response;
	(void)done;

	CrpcController* rpc_controller = dynamic_cast<CrpcController*>(controller);
	if (!rpc_controller) {
		ErrorLog << "call failed. failed to dynamic cast CrpcController";
		setFinished(true);
		return;
	}

	if (!is_pre_set_ || !controller_ || !request_ || !response_) {
		const std::string error_info =
			"must call saveCallee() with controller, request and response before CallMethod()";
		ErrorLog << error_info;
		rpc_controller->setError(ERROR_NOT_SET_ASYNC_PRE_CALL, error_info);
		setFinished(true);
		return;
	}

	// 在切换到工作协程前保存调用链请求号；独立客户端没有 RunTime 时生成新请求号
	RunTime* run_time = GetCurrentRunTime();
	if (run_time && !run_time->msg_no_.empty()) {
		rpc_controller->setMsgReq(run_time->msg_no_);
		DebugLog << "get from RunTime succ, msgno=" << run_time->msg_no_;
	} else if (rpc_controller->msgSeq().empty()) {
		rpc_controller->setMsgReq(MsgReqUtil::genMsgNumber());
		DebugLog << "get from RunTime error, generate new msgno=" << rpc_controller->msgSeq();
	}

	std::shared_ptr<CrpcAsyncChannel> self;
	try {
		self = shared_from_this();
	} catch (const std::bad_weak_ptr&) {
		const std::string error_info =
			"CrpcAsyncChannel must be managed by std::shared_ptr before CallMethod()";
		ErrorLog << error_info;
		rpc_controller->setError(ERROR_NOT_SET_ASYNC_PRE_CALL, error_info);
		setFinished(true);
		return;
	}

	auto rpc_call = [self, method]() mutable {
		DebugLog << "begin to execute async RPC in client worker coroutine";
		self->getRpcChannel()->CallMethod(method, self->getControllerPtr(), self->getRequestPtr(),
										  self->getResponsePtr(), nullptr);
		DebugLog << "finish executing async RPC in client worker coroutine";

		if (self->use_coroutine_wait_) {
			// 服务端协程调用时，回到原始 Reactor 执行 Closure 并恢复调用协程
			auto complete = [self]() mutable {
				self->finishCall();
				self.reset();
			};
			self->getIOThread()->getReactor()->addTask(complete, true);
		} else {
			// 独立客户端没有调用方 Reactor，直接在异步 IO 线程完成回调和通知
			self->finishCall();
		}
		self.reset();
	};

	pending_coroutine_ = GetAsyncClientRuntime()->addCoroutine(rpc_call);
}

void CrpcAsyncChannel::wait() {
	if (use_coroutine_wait_) {
		while (true) {
			{
				std::lock_guard<std::mutex> lock(state_mutex_);
				need_resume_ = true;
				if (is_finished_) {
					return;
				}
			}
			Coroutine::Yield();
		}
	}

	std::unique_lock<std::mutex> lock(state_mutex_);
	need_resume_ = true;
	finished_condition_.wait(lock, [this]() { return is_finished_; });
}

void CrpcAsyncChannel::finishCall() {
	if (getClosurePtr()) {
		getClosurePtr()->Run();
	}

	bool need_resume = false;
	{
		std::lock_guard<std::mutex> lock(state_mutex_);
		is_finished_ = true;
		need_resume = need_resume_;
	}
	finished_condition_.notify_all();

	if (use_coroutine_wait_ && need_resume) {
		Coroutine::Resume(current_coroutine_);
	}
}

void CrpcAsyncChannel::setFinished(bool value) {
	{
		std::lock_guard<std::mutex> lock(state_mutex_);
		is_finished_ = value;
	}
	if (value) {
		finished_condition_.notify_all();
	}
}

bool CrpcAsyncChannel::getNeedResume() const {
	std::lock_guard<std::mutex> lock(state_mutex_);
	return need_resume_;
}

IOThread* CrpcAsyncChannel::getIOThread() {
	return current_io_thread_;
}

Coroutine* CrpcAsyncChannel::getCurrentCoroutine() {
	return current_coroutine_;
}

google::protobuf::RpcController* CrpcAsyncChannel::getControllerPtr() {
	return controller_.get();
}

google::protobuf::Message* CrpcAsyncChannel::getRequestPtr() {
	return request_.get();
}

google::protobuf::Message* CrpcAsyncChannel::getResponsePtr() {
	return response_.get();
}

google::protobuf::Closure* CrpcAsyncChannel::getClosurePtr() {
	return closure_.get();
}

}  // namespace crpc

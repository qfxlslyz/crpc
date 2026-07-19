#ifndef CRPC_RPC_CRPC_ASYNC_CHANNEL_H_
#define CRPC_RPC_CRPC_ASYNC_CHANNEL_H_

#include "crpc/coroutine/coroutine.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/rpc/crpc_channel.h"

#include <condition_variable>
#include <memory>
#include <mutex>

#include <google/protobuf/service.h>

namespace crpc {

/**
 * 非阻塞异步 RPC 通道
 *
 * 独立客户端可以在普通线程中直接使用本类。CallMethod() 会把实际 RPC 调用投递到
 * 内部共享的 IO 线程和工作协程后立即返回，Closure 在异步 IO 线程中执行；wait()
 * 会等待调用完成
 *
 * 当本类在服务端 IO 协程中使用时，完成事件会回到发起调用的 IO 线程，wait()
 * 只挂起当前协程，不阻塞 IO 线程
 *
 * 异步调用期间 request、response、controller 和 closure 必须保持有效，因此调用
 * CallMethod() 前必须通过 saveCallee() 保存这些对象的 shared_ptr
 */
class CrpcAsyncChannel : public google::protobuf::RpcChannel,
						 public std::enable_shared_from_this<CrpcAsyncChannel> {
public:
	using Ptr = std::shared_ptr<CrpcAsyncChannel>;
	using ControllerPtr = std::shared_ptr<google::protobuf::RpcController>;
	using MessagePtr = std::shared_ptr<google::protobuf::Message>;
	using ClosurePtr = std::shared_ptr<google::protobuf::Closure>;

	explicit CrpcAsyncChannel(NetAddress::Ptr addr);
	~CrpcAsyncChannel() override;

	void CallMethod(const google::protobuf::MethodDescriptor* method,
					google::protobuf::RpcController* controller,
					const google::protobuf::Message* request, google::protobuf::Message* response,
					google::protobuf::Closure* done) override;

	CrpcChannel* getRpcChannel();

	// 必须在 CallMethod() 前调用，用 shared_ptr 保证异步调用所需对象的生命周期
	void saveCallee(ControllerPtr controller, MessagePtr request, MessagePtr response,
					ClosurePtr closure);

	// 独立客户端中等待线程；服务端 IO 协程中只挂起当前协程
	void wait();

	void setFinished(bool value);

	bool getNeedResume() const;

	IOThread* getIOThread();

	Coroutine* getCurrentCoroutine();

	google::protobuf::RpcController* getControllerPtr();

	google::protobuf::Message* getRequestPtr();

	google::protobuf::Message* getResponsePtr();

	google::protobuf::Closure* getClosurePtr();

private:
	void finishCall();

private:
	CrpcChannel::Ptr rpc_channel_;
	Coroutine::Ptr pending_coroutine_;
	Coroutine* current_coroutine_{nullptr};
	IOThread* current_io_thread_{nullptr};
	bool use_coroutine_wait_{false};
	bool is_finished_{false};
	bool need_resume_{false};
	bool is_pre_set_{false};

	mutable std::mutex state_mutex_;
	std::condition_variable finished_condition_;

	ControllerPtr controller_;
	MessagePtr request_;
	MessagePtr response_;
	ClosurePtr closure_;
};

}  // namespace crpc

#endif

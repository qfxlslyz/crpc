#include "crpc/base/config.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/tcp_server.h"

#include <map>
#include <memory>

namespace crpc {

extern Config::Ptr rpc_config;

static thread_local IOThread* t_cur_io_thread = nullptr;

IOThread::IOThread() {
	thread_ = std::thread(&IOThread::main, this);

	DebugLog << "wait until new thread finishes "
				"IOThread::main() to init";
	std::unique_lock<std::mutex> lock(state_mutex_);
	state_condition_.wait(lock, [this]() { return initialized_; });
	DebugLog << "finish create io thread";
}

IOThread::~IOThread() {
	{
		std::lock_guard<std::mutex> lock(state_mutex_);
		stopping_ = true;
	}
	if (reactor_) {
		reactor_->stop();
	}
	state_condition_.notify_all();
	if (thread_.joinable()) {
		thread_.join();
	}

	if (reactor_ != nullptr) {
		delete reactor_;
		reactor_ = nullptr;
	}
}

IOThread* IOThread::getCurrentIOThread() {
	return t_cur_io_thread;
}

Reactor* IOThread::getReactor() {
	return reactor_;
}

std::thread::id IOThread::getThreadId() const {
	return thread_.get_id();
}

void IOThread::start() {
	{
		std::lock_guard<std::mutex> lock(state_mutex_);
		if (started_ || stopping_) {
			return;
		}
		started_ = true;
	}
	state_condition_.notify_all();
}

void IOThread::setThreadIndex(const int index) {
	index_ = index;
}

int IOThread::getThreadIndex() {
	return index_;
}

void IOThread::main() {
	Reactor* reactor = new Reactor();
	assert(reactor != nullptr);

	// Reactor 只能在线程内部创建并绑定到该线程，后续 epoll 操作才有明确归属
	t_cur_io_thread = this;
	reactor_ = reactor;
	reactor_->setReactorType(SubReactor);
	tid_ = GetTid();

	Coroutine::getCurrentCoroutine();

	{
		std::unique_lock<std::mutex> lock(state_mutex_);
		initialized_ = true;
		state_condition_.notify_all();
		state_condition_.wait(lock, [this]() { return started_ || stopping_; });
		if (stopping_) {
			return;
		}
	}

	DebugLog << "IOThread " << tid_ << " begin to loop";
	reactor->loop();
}

void IOThread::addClient(TcpConnection* tcp_conn) {
	// 当前版本连接初始化主要在 TcpServer/TcpConnection
	// 中完成，这里保留统一入口
	tcp_conn->registerToTimeWheel();
	tcp_conn->setUpServer();
	return;
}

IOThreadPool::IOThreadPool(int size) : size_(size) {
	io_threads_.resize(size);
	for (int i = 0; i < size; ++i) {
		io_threads_[i] = std::make_shared<IOThread>();
		io_threads_[i]->setThreadIndex(i);
	}
}

void IOThreadPool::start() {
	for (int i = 0; i < size_; ++i) {
		io_threads_[i]->start();
	}
}

IOThread* IOThreadPool::getIOThread() {
	// 简单轮询分配连接，避免所有连接集中到同一个 IO 线程
	if (index_ == size_ || index_ == -1) {
		index_ = 0;
	}
	return io_threads_[index_++].get();
}

int IOThreadPool::getIOThreadPoolSize() {
	return size_;
}

void IOThreadPool::broadcastTask(std::function<void()> cb) {
	// 向所有 IO 线程投递同一个任务，常用于全局状态更新
	for (auto i : io_threads_) {
		i->getReactor()->addTask(cb, true);
	}
}

void IOThreadPool::addTaskByIndex(int index, std::function<void()> cb) {
	if (index >= 0 && index < size_) {
		io_threads_[index]->getReactor()->addTask(cb, true);
	}
}

}  // namespace crpc

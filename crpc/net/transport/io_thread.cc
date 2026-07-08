#include "crpc/base/config.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_pool.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/tcp_server.h"

#include <map>
#include <memory>
#include <stdlib.h>
#include <time.h>

#include <semaphore.h>

namespace crpc {

extern Config::Ptr rpc_config;

static thread_local Reactor* t_reactor_ptr = nullptr;

static thread_local IOThread* t_cur_io_thread = nullptr;

IOThread::IOThread() {
	// 两个信号量分别用于“线程初始化完成”和“允许线程开始 loop”的同步
	int rt = sem_init(&init_semaphore_, 0, 0);
	assert(rt == 0);

	rt = sem_init(&start_semaphore_, 0, 0);
	assert(rt == 0);

	pthread_create(&thread_, nullptr, &IOThread::main, this);

	DebugLog << "semaphore begin to wait until new thread frinish "
				"IOThread::main() to init";
	// 等待新线程完成 IOThread::main() 初始化
	rt = sem_wait(&init_semaphore_);
	assert(rt == 0);
	DebugLog << "semaphore wait end, finish create io thread";

	sem_destroy(&init_semaphore_);
}

IOThread::~IOThread() {
	reactor_->stop();
	pthread_join(thread_, nullptr);

	if (reactor_ != nullptr) {
		delete reactor_;
		reactor_ = nullptr;
	}
}

IOThread* IOThread::getCurrentIOThread() {
	return t_cur_io_thread;
}

sem_t* IOThread::getStartSemaphore() {
	return &start_semaphore_;
}

Reactor* IOThread::getReactor() {
	return reactor_;
}

pthread_t IOThread::getPthreadId() {
	return thread_;
}

void IOThread::setThreadIndex(const int index) {
	index_ = index;
}

int IOThread::getThreadIndex() {
	return index_;
}

void* IOThread::main(void* arg) {
	// assert(t_reactor_ptr == nullptr);

	t_reactor_ptr = new Reactor();
	assert(t_reactor_ptr != nullptr);

	IOThread* thread = static_cast<IOThread*>(arg);
	// Reactor 只能在线程内部创建并绑定到该线程，后续 epoll 操作才有明确归属
	t_cur_io_thread = thread;
	thread->reactor_ = t_reactor_ptr;
	thread->reactor_->setReactorType(SubReactor);
	thread->tid_ = GetTid();

	Coroutine::getCurrentCoroutine();

	DebugLog << "finish iothread init, now post semaphore";
	sem_post(&thread->init_semaphore_);

	// 等待主线程投递 start_semaphore_ 后启动 IOThread 事件循环
	sem_wait(&thread->start_semaphore_);

	sem_destroy(&thread->start_semaphore_);

	DebugLog << "IOThread " << thread->tid_ << " begin to loop";
	t_reactor_ptr->loop();

	return nullptr;
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
		int rt = sem_post(io_threads_[i]->getStartSemaphore());
		assert(rt == 0);
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

void IOThreadPool::addCoroutineToRandomThread(Coroutine::Ptr cor, bool self /* = false*/) {
	if (size_ == 1) {
		io_threads_[0]->getReactor()->addCoroutine(cor, true);
		return;
	}
	srand(time(0));
	int i = 0;
	while (1) {
		i = rand() % (size_);
		// self=false 时尽量避免把协程投递回当前 IO 线程，减少重入风险
		if (!self && io_threads_[i]->getPthreadId() == t_cur_io_thread->getPthreadId()) {
			i++;
			if (i == size_) {
				i -= 2;
			}
		}
		break;
	}
	io_threads_[i]->getReactor()->addCoroutine(cor, true);
	// if (io_threads_[index_]->getPthreadId() ==
	// t_cur_io_thread->getPthreadId())
	// {
	//   index_++;
	//   if (index_ == size_ || index_ == -1) {
	//     index_ = 0;
	//   }
	// }
}

Coroutine::Ptr IOThreadPool::addCoroutineToRandomThread(std::function<void()> cb,
														bool self /* = false*/) {
	Coroutine::Ptr cor = GetCoroutinePool()->getCoroutineInstance();
	cor->setCallBack(cb);
	addCoroutineToRandomThread(cor, self);
	return cor;
}

Coroutine::Ptr IOThreadPool::addCoroutineToThreadByIndex(int index, std::function<void()> cb,
														 bool self /* = false*/) {
	if (index >= (int)io_threads_.size() || index < 0) {
		ErrorLog << "addCoroutineToThreadByIndex error, invalid iothread index[" << index << "]";
		return nullptr;
	}
	Coroutine::Ptr cor = GetCoroutinePool()->getCoroutineInstance();
	cor->setCallBack(cb);
	io_threads_[index]->getReactor()->addCoroutine(cor, true);
	return cor;
}

void IOThreadPool::addCoroutineToEachThread(std::function<void()> cb) {
	for (auto i : io_threads_) {
		Coroutine::Ptr cor = GetCoroutinePool()->getCoroutineInstance();
		cor->setCallBack(cb);
		i->getReactor()->addCoroutine(cor, true);
	}
}

}  // namespace crpc

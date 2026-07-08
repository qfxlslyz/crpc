#ifndef CRPC_NET_TRANSPORT_IO_THREAD_H_
#define CRPC_NET_TRANSPORT_IO_THREAD_H_

#include "crpc/coroutine/coroutine.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"

#include <atomic>
#include <functional>
#include <map>
#include <memory>

#include <semaphore.h>

namespace crpc {

class TcpServer;

/**
 * IO 线程
 *
 * 每个 IOThread 持有一个 SubReactor，负责一批已建立连接的读写事件
 * 构造函数会创建 pthread，并用信号量等待线程内 Reactor 初始化完成
 */
class IOThread {
public:
	using Ptr = std::shared_ptr<IOThread>;
	IOThread();

	~IOThread();

	Reactor* getReactor();

	// 将新连接绑定到当前 IO 线程的 Reactor
	void addClient(TcpConnection* tcp_conn);

	pthread_t getPthreadId();

	void setThreadIndex(const int index);

	int getThreadIndex();

	sem_t* getStartSemaphore();

public:
	static IOThread* getCurrentIOThread();

private:
	static void* main(void* arg);

private:
	Reactor* reactor_{nullptr};	 // 本 IO 线程独占的 SubReactor
	pthread_t thread_{0};
	pid_t tid_{-1};
	TimerEvent::Ptr timer_event_{nullptr};
	int index_{-1};

	sem_t init_semaphore_;	// 通知构造线程：Reactor 已初始化

	sem_t start_semaphore_;	 // 通知 IO 线程：可以进入事件循环
};

/**
 * IO 线程池
 *
 * TcpServer 通过 getIOThread() 轮询选择线程，把新连接分散到多个 SubReactor
 */
class IOThreadPool {
public:
	using Ptr = std::shared_ptr<IOThreadPool>;

	IOThreadPool(int size);

	void start();

	// 轮询返回一个 IO 线程，用于分配新连接
	IOThread* getIOThread();

	int getIOThreadPoolSize();

	void broadcastTask(std::function<void()> cb);

	void addTaskByIndex(int index, std::function<void()> cb);

	void addCoroutineToRandomThread(Coroutine::Ptr cor, bool self = false);

	// 将协程添加到 IO 线程池中的随机线程
	// self 为 false 表示随机到的线程不能是当前线程
	// 请及时释放协程，否则会造成内存泄漏
	// 调用 returnCoroutine(cor) 释放协程
	Coroutine::Ptr addCoroutineToRandomThread(std::function<void()> cb, bool self = false);

	Coroutine::Ptr addCoroutineToThreadByIndex(int index, std::function<void()> cb,
											   bool self = false);

	void addCoroutineToEachThread(std::function<void()> cb);

private:
	int size_{0};

	std::atomic<int> index_{-1};

	std::vector<IOThread::Ptr> io_threads_;
};

}  // namespace crpc
#endif

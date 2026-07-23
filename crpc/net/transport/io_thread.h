#ifndef CRPC_NET_TRANSPORT_IO_THREAD_H_
#define CRPC_NET_TRANSPORT_IO_THREAD_H_

#include "crpc/net/event/reactor.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

namespace crpc {

class TcpServer;

/**
 * IO 线程
 *
 * 每个 IOThread 持有一个 SubReactor，负责一批已建立连接的读写事件
 * 构造函数会创建标准线程，并等待线程内 Reactor 初始化完成
 */
class IOThread {
public:
	using Ptr = std::shared_ptr<IOThread>;
	IOThread();

	~IOThread();

	Reactor* getReactor();

	// 将新连接绑定到当前 IO 线程的 Reactor
	void addClient(TcpConnection* tcp_conn);

	std::thread::id getThreadId() const;

	// 允许已经完成初始化的 IO 线程进入 Reactor 事件循环。
	void start();

	void setThreadIndex(const int index);

	int getThreadIndex();

public:
	static IOThread* getCurrentIOThread();

private:
	void main();

private:
	Reactor* reactor_{nullptr};	 // 本 IO 线程独占的 SubReactor
	std::thread thread_;
	pid_t tid_{-1};
	TimerEvent::Ptr timer_event_{nullptr};
	int index_{-1};

	std::mutex state_mutex_;
	std::condition_variable state_condition_;
	bool initialized_{false};
	bool started_{false};
	bool stopping_{false};
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

private:
	int size_{0};

	std::atomic<int> index_{-1};

	std::vector<IOThread::Ptr> io_threads_;
};

}  // namespace crpc
#endif

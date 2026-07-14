/**
 * Reactor（事件循环）模块
 * 基于 epoll 实现的 Reactor 事件驱动模型，是整个网络库的核心调度引擎
 *
 * 架构: 采用 "主从 Reactor" 模式
 *   - MainReactor: 运行在主线程，负责监听新连接（accept）
 *   - SubReactor:  运行在 IO 线程，负责已建立连接的读写事件
 *
 * 每个 Reactor 内部维护一个 epoll 实例，通过 loop() 进入事件循环:
 *   1. epoll_wait 等待事件就绪
 *   2. 处理到期的定时器事件
 *   3. 执行就绪 fd 的回调（在对应的协程中 Resume）
 *   4. 执行异步任务队列中的任务
 */
#ifndef CRPC_NET_EVENT_REACTOR_H_
#define CRPC_NET_EVENT_REACTOR_H_

#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"

#include <atomic>
#include <functional>
#include <map>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>

namespace crpc {

// Reactor 类型：主从模式
enum ReactorType {
	MainReactor = 1,  // 主 Reactor，只在主线程中使用，负责 accept 新连接
	SubReactor = 2	  // 从 Reactor，每个 IO 线程一个，负责连接的读写
};

class FdEvent;
class Timer;

class Reactor {
public:
	using Ptr = std::shared_ptr<Reactor>;

	explicit Reactor();

	~Reactor();

	// 向 epoll 添加/删除文件描述符的监听事件
	void addEvent(int fd, epoll_event event, bool is_wakeup = true);

	void delEvent(int fd, bool is_wakeup = true);

	// 向 Reactor 投递异步任务（线程安全，可跨线程调用）
	void addTask(std::function<void()> task, bool is_wakeup = true);

	void addTask(std::vector<std::function<void()>> task, bool is_wakeup = true);

	// 将一个协程投递到此 Reactor 中执行
	void addCoroutine(Coroutine::Ptr cor, bool is_wakeup = true);

	// 通过 eventfd 唤醒可能正在 epoll_wait 阻塞的事件循环
	void wakeup();

	// 事件循环主函数（阻塞），不断执行 epoll_wait -> 处理事件 -> 执行任务
	void loop();

	void stop();

	Timer* getTimer();

	void setReactorType(ReactorType type);

public:
	// 获取当前线程绑定的 Reactor（线程局部存储）
	static Reactor* getReactor();

private:
	void addWakeupFd();

	// 检查当前是否在 Reactor 所属的线程中
	bool isLoopThread() const;

	void addEventInLoopThread(int fd, epoll_event event);

	void delEventInLoopThread(int fd);

private:
	int epfd_{-1};		// epoll 文件描述符
	int wake_fd_{-1};	// eventfd，用于跨线程唤醒 epoll_wait
	int timer_fd_{-1};	// timerfd，用于定时器
	bool stop_flag_{false};
	bool is_looping_{false};  // 是否正在执行事件循环
	bool is_init_timer_{false};
	pid_t tid_{0};	// 所属线程 ID

	Mutex mutex_;

	std::vector<int> fds_;	// 已注册监听的 fd 列表
	std::atomic<int> fd_size_;

	// 待操作队列：跨线程投递的事件先放入 pending 队列，由 loop 线程统一处理
	std::map<int, epoll_event> pending_add_fds_;		// 待添加到 epoll 的 fd
	std::vector<int> pending_del_fds_;					// 待从 epoll 删除的 fd
	std::vector<std::function<void()>> pending_tasks_;	// 待执行的异步任务

	Timer* timer_{nullptr};

	ReactorType reactor_type_{SubReactor};
};
}  // namespace crpc
#endif

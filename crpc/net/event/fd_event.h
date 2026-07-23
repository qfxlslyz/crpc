/**
 * 文件描述符事件封装
 * 将 fd 与其 IO 事件回调、所属协程绑定在一起，是 Reactor 管理 IO 的基本单元
 *
 * 每个 FdEvent 关联:
 *   - 一个文件描述符（socket fd）
 *   - 读/写回调函数
 *   - 一个协程（IO 就绪时 Resume 该协程继续执行）
 */
#ifndef CRPC_NET_EVENT_FD_EVENT_H_
#define CRPC_NET_EVENT_FD_EVENT_H_

#include "crpc/base/log.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/net/event/reactor.h"

#include <assert.h>
#include <functional>
#include <memory>
#include <mutex>
#include <sys/epoll.h>
#include <sys/socket.h>

#include <shared_mutex>

namespace crpc {

class Reactor;

// IO 事件类型，直接映射到 epoll 事件标志
enum IOEvent {
	kRead = EPOLLIN,	// 可读事件
	kWrite = EPOLLOUT,	// 可写事件
	ETModel = EPOLLET,	// 边缘触发模式
};

/**
 * fd 事件对象，封装一个 fd 的事件监听与回调分发
 * 当 epoll 通知该 fd 就绪时，Reactor 调用 handleEvent() 执行对应回调
 */
class FdEvent : public std::enable_shared_from_this<FdEvent> {
public:
	using Ptr = std::shared_ptr<FdEvent>;

	FdEvent(Reactor* reactor, int fd = -1);

	FdEvent(int fd);

	virtual ~FdEvent();

	// 根据就绪的事件标志，调用对应的读/写回调
	void handleEvent(int flag);

	void setCallBack(IOEvent flag, std::function<void()> cb);

	std::function<void()> getCallBack(IOEvent flag) const;

	// 添加/删除监听的事件类型
	void addListenEvents(IOEvent event);

	void delListenEvents(IOEvent event);

	// 将当前监听状态同步到 Reactor 的 epoll
	void updateToReactor();

	void unregisterFromReactor();

	int getFd() const;

	void setFd(const int fd);

	int getListenEvents() const;

	Reactor* getReactor() const;

	void setReactor(Reactor* r);

	void setNonBlock();

	bool isNonBlock();

	// 绑定/获取关联的协程（IO 就绪时 Resume 该协程）
	void setCoroutine(Coroutine* cor);

	Coroutine* getCoroutine();

	void clearCoroutine();

protected:
	int fd_{-1};							// 文件描述符
	std::function<void()> read_callback_;	// 可读事件回调
	std::function<void()> write_callback_;	// 可写事件回调

	int listen_events_{0};	// 当前监听的事件掩码

	Reactor* reactor_{nullptr};	 // 所属的 Reactor

	Coroutine* coroutine_{nullptr};	 // 关联的协程
};

/**
 * FdEvent 全局容器（单例）
 * 按 fd 值索引存储 FdEvent 对象，确保同一个 fd 在全局只有一个 FdEvent 实例
 */
class FdEventContainer {
public:
	FdEventContainer(int size);

	FdEvent::Ptr getFdEvent(int fd);

public:
	static FdEventContainer* getFdContainer();

private:
	std::shared_mutex mutex_;
	std::vector<FdEvent::Ptr> fds_;
};

}  // namespace crpc
#endif

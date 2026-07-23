#include "crpc/net/event/fd_event.h"

#include <fcntl.h>
#include <unistd.h>

namespace crpc {

FdEvent::FdEvent(Reactor* reactor, int fd /*=-1*/) : fd_(fd), reactor_(reactor) {
	if (reactor == nullptr) {
		ErrorLog << "create reactor first";
	}
	// assert(reactor != nullptr);
}

FdEvent::FdEvent(int fd) : fd_(fd) {}

FdEvent::~FdEvent() {}

void FdEvent::handleEvent(int flag) {
	// 非协程模式下由 Reactor 直接调用回调；协程模式下通常会 Resume coroutine_
	if (flag == kRead) {
		read_callback_();
	} else if (flag == kWrite) {
		write_callback_();
	} else {
		ErrorLog << "error flag";
	}
}

void FdEvent::setCallBack(IOEvent flag, std::function<void()> cb) {
	// 一个 fd 分别维护读/写两个回调，便于 epoll 同时监听多种事件
	if (flag == kRead) {
		read_callback_ = cb;
	} else if (flag == kWrite) {
		write_callback_ = cb;
	} else {
		ErrorLog << "error flag";
	}
}

std::function<void()> FdEvent::getCallBack(IOEvent flag) const {
	if (flag == kRead) {
		return read_callback_;
	} else if (flag == kWrite) {
		return write_callback_;
	}
	return nullptr;
}

void FdEvent::addListenEvents(IOEvent event) {
	if (listen_events_ & event) {
		DebugLog << "already has this event, skip";
		return;
	}
	listen_events_ |= event;
	// 监听事件变化后立即同步到所属 Reactor
	updateToReactor();
}

void FdEvent::delListenEvents(IOEvent event) {
	if (listen_events_ & event) {
		DebugLog << "delete succ";
		listen_events_ &= ~event;
		updateToReactor();
		return;
	}
	DebugLog << "this event not exist, skip";
}

void FdEvent::updateToReactor() {
	epoll_event event;
	event.events = listen_events_;
	event.data.ptr = this;
	if (!reactor_) {
		// 尚未绑定 Reactor 的 fd 在首次注册事件时绑定到当前线程。
		reactor_ = Reactor::getReactor();
	}

	reactor_->addEvent(fd_, event);
}

void FdEvent::unregisterFromReactor() {
	if (!reactor_) {
		reactor_ = Reactor::getReactor();
	}
	// 取消 epoll 监听后清空回调，避免 fd 复用时误触发旧逻辑
	reactor_->delEvent(fd_);
	listen_events_ = 0;
	read_callback_ = nullptr;
	write_callback_ = nullptr;
}

int FdEvent::getFd() const {
	return fd_;
}

void FdEvent::setFd(const int fd) {
	fd_ = fd;
}

int FdEvent::getListenEvents() const {
	return listen_events_;
}

Reactor* FdEvent::getReactor() const {
	return reactor_;
}

void FdEvent::setReactor(Reactor* r) {
	reactor_ = r;
}

void FdEvent::setNonBlock() {
	if (fd_ == -1) {
		ErrorLog << "error, fd=-1";
		return;
	}

	// hook 后的 IO 依赖非阻塞 fd，EAGAIN 时才能让出协程等待事件
	int flag = fcntl(fd_, F_GETFL, 0);
	if (flag & O_NONBLOCK) {
		DebugLog << "fd already set o_nonblock";
		return;
	}

	fcntl(fd_, F_SETFL, flag | O_NONBLOCK);
	flag = fcntl(fd_, F_GETFL, 0);
	if (flag & O_NONBLOCK) {
		DebugLog << "succ set o_nonblock";
	} else {
		ErrorLog << "set o_nonblock error";
	}
}

bool FdEvent::isNonBlock() {
	if (fd_ == -1) {
		ErrorLog << "error, fd=-1";
		return false;
	}
	int flag = fcntl(fd_, F_GETFL, 0);
	return (flag & O_NONBLOCK);
}

void FdEvent::setCoroutine(Coroutine* cor) {
	coroutine_ = cor;
}

void FdEvent::clearCoroutine() {
	coroutine_ = nullptr;
}

Coroutine* FdEvent::getCoroutine() {
	return coroutine_;
}

FdEvent::Ptr FdEventContainer::getFdEvent(int fd) {
	// fd 值通常较小，优先读锁快速命中已有对象
	std::shared_lock<std::shared_mutex> rlock(mutex_);
	if (fd < static_cast<int>(fds_.size())) {
		FdEvent::Ptr re = fds_[fd];
		rlock.unlock();
		return re;
	}
	rlock.unlock();

	// 容器不够大时按比例扩容，确保 fds_[fd] 可直接索引
	std::unique_lock<std::shared_mutex> wlock(mutex_);
	int n = (int)(fd * 1.5);
	for (int i = fds_.size(); i < n; ++i) {
		fds_.push_back(std::make_shared<FdEvent>(i));
	}
	FdEvent::Ptr re = fds_[fd];
	wlock.unlock();
	return re;
}

FdEventContainer::FdEventContainer(int size) {
	for (int i = 0; i < size; ++i) {
		fds_.push_back(std::make_shared<FdEvent>(i));
	}
}

FdEventContainer* FdEventContainer::getFdContainer() {
	static FdEventContainer container(1000);
	return &container;
}

}  // namespace crpc

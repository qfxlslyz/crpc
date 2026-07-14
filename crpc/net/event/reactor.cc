#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/event/timer.h"

#include <algorithm>
#include <assert.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

extern ReadFunPtr g_sys_read_fun;	 // 原始 read 函数
extern WriteFunPtr g_sys_write_fun;	 // 原始 write 函数

namespace crpc {

static thread_local Reactor* t_reactor_ptr = nullptr;

static thread_local int t_max_epoll_timeout = 10000;  // 毫秒

Reactor::Reactor() {
	// 一个线程不能创建多个 Reactor 对象
	// assert(t_reactor_ptr == nullptr);
	if (t_reactor_ptr != nullptr) {
		ErrorLog << "this thread has already create a reactor";
		Exit(0);
	}

	tid_ = GetTid();

	DebugLog << "thread[" << tid_ << "] succ create a reactor object";
	t_reactor_ptr = this;

	if ((epfd_ = epoll_create(1)) <= 0) {
		ErrorLog << "Start server error. epoll_create error, sys error=" << strerror(errno);
		Exit(0);
	} else {
		DebugLog << "epfd_ = " << epfd_;
	}
	// assert(epfd_ > 0);

	if ((wake_fd_ = eventfd(0, EFD_NONBLOCK)) <= 0) {
		ErrorLog << "Start server error. event_fd error, sys error=" << strerror(errno);
		Exit(0);
	}
	DebugLog << "wakefd = " << wake_fd_;
	// assert(wake_fd_ > 0);
	addWakeupFd();
}

Reactor::~Reactor() {
	DebugLog << "~Reactor";
	close(epfd_);
	if (timer_ != nullptr) {
		delete timer_;
		timer_ = nullptr;
	}
	t_reactor_ptr = nullptr;
}

Reactor* Reactor::getReactor() {
	if (t_reactor_ptr == nullptr) {
		DebugLog << "Create new Reactor";
		t_reactor_ptr = new Reactor();
	}
	// DebugLog << "t_reactor_ptr = " << t_reactor_ptr;
	return t_reactor_ptr;
}

// 由其他线程调用，需要加锁
void Reactor::addEvent(int fd, epoll_event event, bool is_wakeup /*=true*/) {
	if (fd == -1) {
		ErrorLog << "add error. fd invalid, fd = -1";
		return;
	}
	if (isLoopThread()) {
		// Reactor 所属线程内可以直接操作 epoll，避免额外排队
		addEventInLoopThread(fd, event);
		return;
	}
	{
		// 跨线程操作先进入 pending 队列，由 loop 线程统一执行 epoll_ctl
		Mutex::ScopedLock lock(mutex_);
		pending_add_fds_.insert(std::pair<int, epoll_event>(fd, event));
	}
	if (is_wakeup) {
		wakeup();
	}
}

// 由其他线程调用，需要加锁
void Reactor::delEvent(int fd, bool is_wakeup /*=true*/) {
	if (fd == -1) {
		ErrorLog << "add error. fd invalid, fd = -1";
		return;
	}

	if (isLoopThread()) {
		// 同线程删除可立即执行
		delEventInLoopThread(fd);
		return;
	}

	{
		// 跨线程删除同样排队，保证 epoll_ctl 都发生在 Reactor 所属线程
		Mutex::ScopedLock lock(mutex_);
		pending_del_fds_.push_back(fd);
	}
	if (is_wakeup) {
		wakeup();
	}
}

void Reactor::wakeup() {
	if (!is_looping_) {
		return;
	}

	uint64_t tmp = 1;
	uint64_t* p = &tmp;
	// eventfd 写入任意 8 字节整数即可唤醒 epoll_wait
	if (g_sys_write_fun(wake_fd_, p, 8) != 8) {
		ErrorLog << "write wakeupfd[" << wake_fd_ << "] error";
	}
}

// tid_ 只会在 Reactor 构造函数中写入，因此无需加锁
bool Reactor::isLoopThread() const {
	if (tid_ == GetTid()) {
		return true;
	}
	// DebugLog << "tid_ = "<< tid_ << ", getttid = " << GetTid() <<"return
	// false";
	return false;
}

void Reactor::addWakeupFd() {
	// wake_fd_ 专门用于跨线程唤醒，不绑定 FdEvent 回调
	int op = EPOLL_CTL_ADD;
	epoll_event event;
	event.data.fd = wake_fd_;
	event.events = EPOLLIN;
	if ((epoll_ctl(epfd_, op, wake_fd_, &event)) != 0) {
		ErrorLog << "epoo_ctl error, fd[" << wake_fd_ << "], errno=" << errno
				 << ", err=" << strerror(errno);
	}
	fds_.push_back(wake_fd_);
}

// 只在当前线程调用，无需加锁
void Reactor::addEventInLoopThread(int fd, epoll_event event) {
	assert(isLoopThread());

	int op = EPOLL_CTL_ADD;
	bool is_add = true;
	// int tmp_fd = event;
	auto it = find(fds_.begin(), fds_.end(), fd);
	if (it != fds_.end()) {
		is_add = false;
		op = EPOLL_CTL_MOD;
	}

	// epoll_event event;
	// event.data.ptr = fd_event.get();
	// event.events = fd_event->getListenEvents();

	if (epoll_ctl(epfd_, op, fd, &event) != 0) {
		ErrorLog << "epoo_ctl error, fd[" << fd << "], sys errinfo = " << strerror(errno);
		return;
	}
	if (is_add) {
		fds_.push_back(fd);
	}
	DebugLog << "epoll_ctl add succ, fd[" << fd << "]";
}

// 只在当前线程调用，无需加锁
void Reactor::delEventInLoopThread(int fd) {
	assert(isLoopThread());

	auto it = find(fds_.begin(), fds_.end(), fd);
	if (it == fds_.end()) {
		DebugLog << "fd[" << fd << "] not in this loop";
		return;
	}
	int op = EPOLL_CTL_DEL;

	if ((epoll_ctl(epfd_, op, fd, nullptr)) != 0) {
		ErrorLog << "epoo_ctl error, fd[" << fd << "], sys errinfo = " << strerror(errno);
	}

	fds_.erase(it);
	DebugLog << "del succ, fd[" << fd << "]";
}

void Reactor::loop() {
	assert(isLoopThread());
	if (is_looping_) {
		return;
	}

	is_looping_ = true;
	stop_flag_ = false;

	while (!stop_flag_) {
		const int MAX_EVENTS = 100;
		epoll_event re_events[MAX_EVENTS + 1];

		// 执行外部线程投递的普通任务，例如添加协程、刷新时间轮等
		Mutex::ScopedLock lock(mutex_);
		std::vector<std::function<void()>> tmp_tasks;
		tmp_tasks.swap(pending_tasks_);
		lock.unlock();

		for (size_t i = 0; i < tmp_tasks.size(); ++i) {
			if (tmp_tasks[i]) {
				tmp_tasks[i]();
			}
		}

		// 等待 fd 就绪；超时时间较长，主要依赖 eventfd/timerfd 唤醒
		int rt = epoll_wait(epfd_, re_events, MAX_EVENTS, t_max_epoll_timeout);

		if (rt < 0) {
			ErrorLog << "epoll_wait error, skip, errno=" << strerror(errno);
			continue;
		}

		// 先完整扫描本批 epoll 事件，再统一恢复就绪协程，
		// 避免协程执行过程中对 Reactor 状态的修改干扰事件遍历。
		std::vector<Coroutine*> ready_coroutines;
		ready_coroutines.reserve(rt);

		for (int i = 0; i < rt; ++i) {
			epoll_event one_event = re_events[i];

			// 唤醒事件 必须把 eventfd 读空，否则会持续触发可读事件
			if (one_event.data.fd == wake_fd_ && (one_event.events & kRead)) {
				char buf[8];
				while (1) {
					if ((g_sys_read_fun(wake_fd_, buf, 8) == -1) && errno == EAGAIN) {
						break;
					}
				}
				continue;
			}

			FdEvent* ptr = (FdEvent*)one_event.data.ptr;
			if (!ptr) {
				continue;
			}
			int fd = ptr->getFd();

			if ((!(one_event.events & EPOLLIN)) && (!(one_event.events & EPOLLOUT))) {
				ErrorLog << "socket [" << fd << "] occur other unknow event:[" << one_event.events
						 << "], need unregister this socket";
				delEventInLoopThread(fd);
				continue;
			}

			// 协程在本批事件遍历完成后由当前 Reactor 线程依次恢复。
			if (Coroutine* coroutine = ptr->getCoroutine()) {
				ready_coroutines.push_back(coroutine);
				continue;
			}
			std::function<void()> read_cb;
			std::function<void()> write_cb;
			read_cb = ptr->getCallBack(kRead);
			write_cb = ptr->getCallBack(kWrite);
			// 如果是定时器事件，直接执行
			if (fd == timer_fd_) {
				// 定时器回调会读取 timerfd
				// 并执行已到期任务
				read_cb();
				continue;
			}
			if (one_event.events & EPOLLIN) {
				// 普通回调放入任务队列，避免在 epoll
				// 事件遍历中执行过重逻辑
				Mutex::ScopedLock lock(mutex_);
				pending_tasks_.push_back(read_cb);
			}
			if (one_event.events & EPOLLOUT) {
				Mutex::ScopedLock lock(mutex_);
				pending_tasks_.push_back(write_cb);
			}
		}

		std::map<int, epoll_event> tmp_add;
		std::vector<int> tmp_del;

		{
			Mutex::ScopedLock lock(mutex_);
			// 将本轮积累的 epoll 变更取出，统一在 Reactor 线程执行
			tmp_add.swap(pending_add_fds_);
			pending_add_fds_.clear();

			tmp_del.swap(pending_del_fds_);
			pending_del_fds_.clear();
		}
		for (auto i = tmp_add.begin(); i != tmp_add.end(); ++i) {
			addEventInLoopThread((*i).first, (*i).second);
		}
		for (auto i = tmp_del.begin(); i != tmp_del.end(); ++i) {
			delEventInLoopThread((*i));
		}

		for (Coroutine* coroutine : ready_coroutines) {
			Coroutine::Resume(coroutine);
		}
	}
	DebugLog << "reactor loop end";
	is_looping_ = false;
}

void Reactor::stop() {
	if (!stop_flag_ && is_looping_) {
		stop_flag_ = true;
		wakeup();
	}
}

void Reactor::addTask(std::function<void()> task, bool is_wakeup /*=true*/) {
	{
		// 任务可能来自任意线程，因此统一加锁后交给 loop 线程执行
		Mutex::ScopedLock lock(mutex_);
		pending_tasks_.push_back(task);
	}
	if (is_wakeup) {
		wakeup();
	}
}

void Reactor::addTask(std::vector<std::function<void()>> task, bool is_wakeup /* =true*/) {
	if (task.size() == 0) {
		return;
	}

	{
		Mutex::ScopedLock lock(mutex_);
		pending_tasks_.insert(pending_tasks_.end(), task.begin(), task.end());
	}
	if (is_wakeup) {
		wakeup();
	}
}

void Reactor::addCoroutine(Coroutine::Ptr cor, bool is_wakeup /*=true*/) {
	// 协程本质上也是一个待执行任务：在 Reactor 线程中 Resume 它
	auto func = [cor]() { Coroutine::Resume(cor.get()); };
	addTask(func, is_wakeup);
}

Timer* Reactor::getTimer() {
	if (!timer_) {
		// Timer 继承 FdEvent，创建时会把 timerfd 注册到当前 Reactor
		timer_ = new Timer(this);
		timer_fd_ = timer_->getFd();
	}
	return timer_;
}

void Reactor::setReactorType(ReactorType type) {
	reactor_type_ = type;
}

}  // namespace crpc

#include "crpc/base/log.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/timer.h"

#include <assert.h>
#include <functional>
#include <map>
#include <string.h>
#include <sys/timerfd.h>
#include <time.h>
#include <vector>

extern ReadFunPtr g_sys_read_fun;  // 原始 read 函数

namespace crpc {

int64_t GetNowMs() {
	timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
		ErrorLog << "clock_gettime error, errno=" << errno << ", error=" << strerror(errno);
		return 0;
	}
	return static_cast<int64_t>(now.tv_sec) * 1000 + now.tv_nsec / 1000000;
}

Timer::Timer(Reactor* reactor) : FdEvent(reactor) {
	// 使用单调时钟，避免系统时间被调整影响定时器触发
	fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
	DebugLog << "timer_ fd = " << fd_;
	if (fd_ == -1) {
		DebugLog << "timerfd_create error";
	}
	// timerfd 可读时说明最近的定时事件到期
	read_callback_ = std::bind(&Timer::onTimer, this);
	addListenEvents(kRead);
	// 将事件更新到 Reactor
}

Timer::~Timer() {
	unregisterFromReactor();
	close(fd_);
}

void Timer::addTimerEvent(TimerEvent::Ptr event, bool need_reset /*=true*/) {
	std::unique_lock<std::shared_mutex> lock(event_mutex_);
	bool is_reset = false;
	// 如果新事件比当前最近事件更早，需要重新设置 timerfd 到期时间
	if (pending_events_.empty()) {
		is_reset = true;
	} else {
		auto it = pending_events_.begin();
		if (event->arrive_time_ < it->second->arrive_time_) {
			is_reset = true;
		}
	}
	pending_events_.emplace(event->arrive_time_, event);
	lock.unlock();

	if (is_reset && need_reset) {
		DebugLog << "need reset timer";
		resetArriveTime();
	}
}

void Timer::delTimerEvent(TimerEvent::Ptr event) {
	if (!event) {
		return;
	}

	std::unique_lock<std::shared_mutex> lock(event_mutex_);
	// 先标记取消，即使当前未能从 multimap 找到，也可避免稍后被执行
	event->is_canceled_ = true;

	auto begin = pending_events_.lower_bound(event->arrive_time_);
	auto end = pending_events_.upper_bound(event->arrive_time_);
	auto it = begin;
	for (; it != end; ++it) {
		if (it->second == event) {
			DebugLog << "find timer event, now delete it. src arrive time=" << event->arrive_time_;
			break;
		}
	}

	bool need_reset = false;
	if (it != end) {
		need_reset = (it == pending_events_.begin());
		pending_events_.erase(it);
	}
	lock.unlock();

	if (need_reset) {
		resetArriveTime();
	}
	DebugLog << "del timer event finish, origin arrive time=" << event->arrive_time_;
}

void Timer::resetArriveTime() {
	// 使用独占锁将“读取最近事件”和“设置 timerfd”串行化，避免并发 add/del
	// 最终用旧快照覆盖较新的到期时间
	std::unique_lock<std::shared_mutex> lock(event_mutex_);

	itimerspec new_value;
	memset(&new_value, 0, sizeof(new_value));

	if (pending_events_.empty()) {
		// it_value 为 0 会解除 timerfd，避免删除最后一个事件后仍收到旧唤醒
		if (timerfd_settime(fd_, 0, &new_value, nullptr) != 0) {
			ErrorLog << "timerfd_settime disarm error, errno=" << errno
					 << ", error=" << strerror(errno);
		}
		DebugLog << "no timerevent pending, size = 0";
		return;
	}

	int64_t now = GetNowMs();
	// multimap 的 begin() 是最近到期事件
	const int64_t arrive_time = pending_events_.begin()->first;
	const int64_t interval = arrive_time > now ? arrive_time - now : 0;

	new_value.it_value.tv_sec = interval / 1000;
	new_value.it_value.tv_nsec = (interval % 1000) * 1000000;
	if (interval == 0) {
		// timerfd 的全零 it_value 表示解除定时器，因此已到期事件用 1ns 立即触发
		new_value.it_value.tv_nsec = 1;
	}

	int rt = timerfd_settime(fd_, 0, &new_value, nullptr);

	if (rt != 0) {
		ErrorLog << "timerfd_settime error, interval=" << interval << ", errno=" << errno
				 << ", error=" << strerror(errno);
	}
}

void Timer::onTimer() {
	// 先读空 timerfd，清除可读状态
	char buf[8];
	while (1) {
		ssize_t read_count = g_sys_read_fun(fd_, buf, 8);
		if (read_count == 8) {
			continue;
		}
		if (read_count == -1 && errno == EINTR) {
			continue;
		}
		if (read_count == -1 && errno != EAGAIN) {
			ErrorLog << "read timerfd error, errno=" << errno << ", error=" << strerror(errno);
		}
		if (read_count <= 0) {
			break;
		}
	}

	int64_t now = GetNowMs();
	std::unique_lock<std::shared_mutex> lock(event_mutex_);
	auto it = pending_events_.begin();
	std::vector<TimerEvent::Ptr> expired_events;
	std::vector<std::pair<int64_t, std::function<void()>>> tasks;
	for (it = pending_events_.begin(); it != pending_events_.end(); it++) {
		// multimap 按到期时间排序，遇到第一个未到期事件即可停止
		if (it->first > now) {
			break;
		}
		if (!(it->second->is_canceled_)) {
			expired_events.push_back(it->second);
			tasks.emplace_back(it->second->arrive_time_, it->second->task_);
		}
	}

	pending_events_.erase(pending_events_.begin(), it);
	lock.unlock();

	for (const auto& expired_event : expired_events) {
		// 周期性事件执行前先计算下一次到期时间并重新加入队列
		if (expired_event->is_repeated_) {
			expired_event->resetTime();
			addTimerEvent(expired_event, false);
		}
	}

	resetArriveTime();

	// 任务在 Reactor 线程中直接执行，避免额外排队造成定时误差
	for (const auto& task : tasks) {
		task.second();
	}
}

}  // namespace crpc

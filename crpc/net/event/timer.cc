#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/timer.h"

#include <assert.h>
#include <functional>
#include <map>
#include <string.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <time.h>
#include <vector>

extern ReadFunPtr g_sys_read_fun;  // 原始 read 函数

namespace crpc {

int64_t GetNowMs() {
	timeval val;
	gettimeofday(&val, nullptr);
	int64_t re = val.tv_sec * 1000 + val.tv_usec / 1000;
	return re;
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
	RWMutex::WriteScopedLock lock(event_mutex_);
	bool is_reset = false;
	// 如果新事件比当前最近事件更早，需要重新设置 timerfd 到期时间
	if (pending_events_.empty()) {
		is_reset = true;
	} else {
		auto it = pending_events_.begin();
		if (event->arrive_time_ < (*it).second->arrive_time_) {
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
	// 先标记取消，即使当前未能从 multimap 找到，也可避免稍后被执行
	event->is_canceled_ = true;

	RWMutex::WriteScopedLock lock(event_mutex_);
	auto begin = pending_events_.lower_bound(event->arrive_time_);
	auto end = pending_events_.upper_bound(event->arrive_time_);
	auto it = begin;
	for (it = begin; it != end; it++) {
		if (it->second == event) {
			DebugLog << "find timer event, now delete it. src arrive time=" << event->arrive_time_;
			break;
		}
	}
	if (it != pending_events_.end()) {
		pending_events_.erase(it);
	}
	lock.unlock();
	DebugLog << "del timer event succ, origin arrvite time=" << event->arrive_time_;
}

void Timer::resetArriveTime() {
	RWMutex::ReadScopedLock lock(event_mutex_);
	std::multimap<int64_t, TimerEvent::Ptr> tmp = pending_events_;
	lock.unlock();

	if (tmp.size() == 0) {
		DebugLog << "no timerevent pending, size = 0";
		return;
	}

	int64_t now = GetNowMs();
	auto it = tmp.rbegin();
	if ((*it).first < now) {
		DebugLog << "all timer events has already expire";
		return;
	}
	int64_t interval = (*it).first - now;

	itimerspec new_value;
	memset(&new_value, 0, sizeof(new_value));

	timespec ts;
	memset(&ts, 0, sizeof(ts));
	ts.tv_sec = interval / 1000;
	ts.tv_nsec = (interval % 1000) * 1000000;
	new_value.it_value = ts;

	int rt = timerfd_settime(fd_, 0, &new_value, nullptr);

	if (rt != 0) {
		ErrorLog << "tiemr_settime error, interval=" << interval;
	} else {
	}
}

void Timer::onTimer() {
	// 先读空 timerfd，清除可读状态
	char buf[8];
	while (1) {
		if ((g_sys_read_fun(fd_, buf, 8) == -1) && errno == EAGAIN) {
			break;
		}
	}

	int64_t now = GetNowMs();
	RWMutex::WriteScopedLock lock(event_mutex_);
	auto it = pending_events_.begin();
	std::vector<TimerEvent::Ptr> tmps;
	std::vector<std::pair<int64_t, std::function<void()>>> tasks;
	for (it = pending_events_.begin(); it != pending_events_.end(); ++it) {
		// multimap 按到期时间排序，遇到第一个未到期事件即可停止
		if ((*it).first <= now && !((*it).second->is_canceled_)) {
			tmps.push_back((*it).second);
			tasks.push_back(std::make_pair((*it).second->arrive_time_, (*it).second->task_));
		} else {
			break;
		}
	}

	pending_events_.erase(pending_events_.begin(), it);
	lock.unlock();

	for (auto i = tmps.begin(); i != tmps.end(); ++i) {
		// 周期性事件执行前先计算下一次到期时间并重新加入队列
		if ((*i)->is_repeated_) {
			(*i)->resetTime();
			addTimerEvent(*i, false);
		}
	}

	resetArriveTime();

	// 任务在 Reactor 线程中直接执行，避免额外排队造成定时误差
	for (auto i : tasks) {
		i.second();
	}
}

}  // namespace crpc

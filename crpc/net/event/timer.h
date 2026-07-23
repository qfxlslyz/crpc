/**
 * 定时器模块
 * 基于 Linux timerfd 实现，与 epoll 事件循环无缝集成
 *
 * 用途: 日志定时刷盘、连接超时检测、时间轮驱动等
 * 实现: Timer 继承自 FdEvent，timerfd 到期时触发可读事件，
 *       在回调中遍历到期的 TimerEvent 并执行其任务函数
 */
#ifndef CRPC_NET_EVENT_TIMER_H_
#define CRPC_NET_EVENT_TIMER_H_

#include "crpc/base/log.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/reactor.h"

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <time.h>

#include <shared_mutex>

namespace crpc {

// 获取 CLOCK_MONOTONIC 单调时钟的毫秒值
int64_t GetNowMs();

/**
 * 定时事件：封装一个延迟/周期执行的任务
 */
class TimerEvent {
public:
	using Ptr = std::shared_ptr<TimerEvent>;
	TimerEvent(int64_t interval, bool is_repeated, std::function<void()> task)
		: interval_(interval), is_repeated_(is_repeated), task_(task) {
		arrive_time_ = GetNowMs() + interval_;
		DebugLog << "timeevent will occur at " << arrive_time_;
	}

	// 重置触发时间为"当前时间 + 间隔"
	void resetTime() {
		arrive_time_ = GetNowMs() + interval_;
		is_canceled_ = false;
	}

	void wake() { is_canceled_ = false; }

	void cancle() { is_canceled_ = true; }

	void cancleRepeated() { is_repeated_ = false; }

public:
	int64_t arrive_time_;		  // 单调时钟上的下次触发时间点（毫秒）
	int64_t interval_;			  // 触发间隔（毫秒）
	bool is_repeated_{false};	  // 是否周期性重复执行
	bool is_canceled_{false};	  // 是否已取消
	std::function<void()> task_;  // 到期时执行的回调函数
};

class FdEvent;

/**
 * 定时器，继承 FdEvent，内部维护一个 timerfd
 * 使用 multimap 按触发时间排序存储所有定时事件，
 * timerfd 到期后在 onTimer() 中批量执行所有已到期的事件
 */
class Timer : public FdEvent {
public:
	using Ptr = std::shared_ptr<Timer>;

	Timer(Reactor* reactor);

	~Timer() override;

	void addTimerEvent(TimerEvent::Ptr event, bool need_reset = true);

	void delTimerEvent(TimerEvent::Ptr event);

	// 重新设置 timerfd 的超时时间为最近一个待触发事件的时间
	void resetArriveTime();

	// timerfd 可读回调: 遍历并执行所有已到期的定时事件
	void onTimer();

private:
	// 按触发时间排序的定时事件集合（multimap 支持同一时刻多个事件）
	std::multimap<int64_t, TimerEvent::Ptr> pending_events_;
	std::shared_mutex event_mutex_;
};

}  // namespace crpc
#endif

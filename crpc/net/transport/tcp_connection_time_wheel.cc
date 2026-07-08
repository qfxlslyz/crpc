#include "crpc/net/event/timer.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/timeout_slot.h"

#include <queue>
#include <vector>

namespace crpc {

TcpTimeWheel::TcpTimeWheel(Reactor* reactor, int bucket_count, int interval /*= 10*/)
	: reactor_(reactor), bucket_count_(bucket_count), interval_(interval) {
	for (int i = 0; i < bucket_count; ++i) {
		std::vector<TcpConnectionSlot::Ptr> tmp;
		wheel_.push(tmp);
	}

	event_ = std::make_shared<TimerEvent>(interval_ * 1000, true,
										  std::bind(&TcpTimeWheel::loopFunc, this));
	reactor_->getTimer()->addTimerEvent(event_);
}

TcpTimeWheel::~TcpTimeWheel() {
	reactor_->getTimer()->delTimerEvent(event_);
}

void TcpTimeWheel::loopFunc() {
	wheel_.pop();
	std::vector<TcpConnectionSlot::Ptr> tmp;
	wheel_.push(tmp);
}

void TcpTimeWheel::fresh(TcpConnectionSlot::Ptr slot) {
	DebugLog << "fresh connection";
	wheel_.back().emplace_back(slot);
}

}  // namespace crpc

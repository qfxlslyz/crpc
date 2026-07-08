#ifndef CRPC_NET_TRANSPORT_TCP_CONNECTION_TIME_WHEEL_H_
#define CRPC_NET_TRANSPORT_TCP_CONNECTION_TIME_WHEEL_H_

#include "crpc/net/event/reactor.h"
#include "crpc/net/event/timer.h"
#include "crpc/net/transport/timeout_slot.h"

#include <queue>
#include <vector>

namespace crpc {

class TcpConnection;

class TcpTimeWheel {
public:
	using Ptr = std::shared_ptr<TcpTimeWheel>;

	using TcpConnectionSlot = TimeoutSlot<TcpConnection>;

	TcpTimeWheel(Reactor* reactor, int bucket_count, int invetal = 10);

	~TcpTimeWheel();

	void fresh(TcpConnectionSlot::Ptr slot);

	void loopFunc();

private:
	Reactor* reactor_{nullptr};
	int bucket_count_{0};
	int interval_{0};  // 秒

	TimerEvent::Ptr event_;
	std::queue<std::vector<TcpConnectionSlot::Ptr>> wheel_;
};

}  // namespace crpc

#endif

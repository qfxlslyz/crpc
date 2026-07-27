#include "crpc/net/event/timer.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/timeout_slot.h"

#include <queue>
#include <vector>

namespace crpc {

TcpTimeWheel::TcpTimeWheel(Reactor* reactor, int bucket_count, int interval /*= 10*/)
	: reactor_(reactor), bucket_count_(bucket_count), interval_(interval) {
	// 预先建立固定数量的桶；时间轮覆盖的空闲窗口约为
	// bucket_count_ * interval_ 秒，检测精度为 interval_ 秒
	for (int i = 0; i < bucket_count_; ++i) {
		wheel_.emplace();
	}

	// Timer 只提供周期 tick，具体连接是否过期由桶轮转和 Slot 引用计数决定
	event_ = std::make_shared<TimerEvent>(interval_ * 1000, true,
										  std::bind(&TcpTimeWheel::loopFunc, this));
	reactor_->getTimer()->addTimerEvent(event_);
}

TcpTimeWheel::~TcpTimeWheel() {
	reactor_->getTimer()->delTimerEvent(event_);
}

void TcpTimeWheel::loopFunc() {
	// 队首桶出队时释放其中的 Slot 引用。若某个 Slot 后续没有被 fresh() 到
	// 更靠后的桶，它的最后一个 shared_ptr 会在这里销毁并触发连接关闭回调
	wheel_.pop();
	wheel_.emplace();
}

void TcpTimeWheel::fresh(TcpConnectionSlot::Ptr slot) {
	DebugLog << "fresh connection";
	// 同一个 Slot 可以同时存在于多个桶中。追加到队尾相当于续期；旧桶中的引用
	// 无需主动删除，之后随桶出队自然释放
	wheel_.back().emplace_back(slot);
}

}  // namespace crpc

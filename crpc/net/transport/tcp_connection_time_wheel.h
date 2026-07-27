#ifndef CRPC_NET_TRANSPORT_TCP_CONNECTION_TIME_WHEEL_H_
#define CRPC_NET_TRANSPORT_TCP_CONNECTION_TIME_WHEEL_H_

#include "crpc/net/event/reactor.h"
#include "crpc/net/event/timer.h"
#include "crpc/net/transport/timeout_slot.h"

#include <queue>
#include <vector>

namespace crpc {

class TcpConnection;

/**
 * 服务端空闲连接时间轮
 *
 * 时间轮由 bucket_count 个桶组成，每隔 interval 秒向前推进一格：
 *   1. 丢弃队首桶；
 *   2. 在队尾补入一个空桶；
 *   3. 新建或有数据交互的连接把同一个 TimeoutSlot 追加到队尾桶。
 *
 * 刷新连接时不会从旧桶中查找并删除原记录，而是依靠 shared_ptr 引用计数续期。
 * 只要较新的桶仍持有同一个 TimeoutSlot，旧桶出队就不会析构该 Slot；连接持续
 * 空闲约 bucket_count * interval 秒后，最后一个桶引用被释放，TimeoutSlot 的
 * 析构回调才会关闭连接
 *
 * TimerEvent 只负责按 interval 周期驱动时间轮，适合批量、低精度的连接空闲检测，
 * 避免为每条 TCP 连接单独创建一个定时事件。运行期的桶轮转和连接刷新由
 * MainReactor 所属线程串行执行；跨线程刷新由 TcpServer 投递到该线程
 */
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
	// 队首是下一次 tick 将过期的桶，队尾接收新建或刚刷新的连接槽位
	std::queue<std::vector<TcpConnectionSlot::Ptr>> wheel_;
};

}  // namespace crpc

#endif

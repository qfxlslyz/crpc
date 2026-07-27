#ifndef CRPC_NET_TRANSPORT_TIMEOUT_SLOT_H_
#define CRPC_NET_TRANSPORT_TIMEOUT_SLOT_H_

#include <functional>
#include <memory>

namespace crpc {

/**
 * 将“对象仍然存活时执行超时回调”绑定到 Slot 的最后一次引用释放。
 *
 * Slot 只弱引用业务对象，不延长其生命周期；在时间轮中，同一个 Slot 可以被多个
 * 桶共同持有。只有所有桶都释放该 Slot 后析构函数才执行，因此向新桶追加同一个
 * Slot 就能实现续期，而不需要在旧桶中删除记录。
 */
template <class T>
class TimeoutSlot {
public:
	using Ptr = std::shared_ptr<TimeoutSlot>;
	using WeakPtr = std::weak_ptr<T>;
	using SharedPtr = std::shared_ptr<T>;

	TimeoutSlot(WeakPtr ptr, std::function<void(SharedPtr)> cb) : weak_ptr_(ptr), cb_(cb) {}
	~TimeoutSlot() {
		// 对象若已通过其他路径销毁，则无需再执行超时处理。
		SharedPtr ptr = weak_ptr_.lock();
		if (ptr) {
			cb_(ptr);
		}
	}

private:
	WeakPtr weak_ptr_;
	std::function<void(SharedPtr)> cb_;
};

}  // namespace crpc
#endif

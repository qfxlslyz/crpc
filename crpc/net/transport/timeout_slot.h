#ifndef CRPC_NET_TRANSPORT_TIMEOUT_SLOT_H_
#define CRPC_NET_TRANSPORT_TIMEOUT_SLOT_H_

#include <functional>
#include <memory>

namespace crpc {

template <class T>
class TimeoutSlot {
public:
	using Ptr = std::shared_ptr<TimeoutSlot>;
	using WeakPtr = std::weak_ptr<T>;
	using SharedPtr = std::shared_ptr<T>;

	TimeoutSlot(WeakPtr ptr, std::function<void(SharedPtr)> cb) : weak_ptr_(ptr), cb_(cb) {}
	~TimeoutSlot() {
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
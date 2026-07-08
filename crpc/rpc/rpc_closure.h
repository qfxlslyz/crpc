#ifndef CRPC_RPC_RPC_CLOSURE_H_
#define CRPC_RPC_RPC_CLOSURE_H_

#include <functional>
#include <memory>

#include <google/protobuf/stubs/callback.h>

namespace crpc {

class RpcClosure : public google::protobuf::Closure {
public:
	using Ptr = std::shared_ptr<RpcClosure>;
	explicit RpcClosure(std::function<void()> cb) : cb_(cb) {}

	~RpcClosure() override = default;

	void Run() override {
		if (cb_) {
			cb_();
		}
	}

private:
	std::function<void()> cb_{nullptr};
};

}  // namespace crpc

#endif

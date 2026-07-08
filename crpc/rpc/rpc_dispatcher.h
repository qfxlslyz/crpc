#ifndef CRPC_RPC_RPC_DISPATCHER_H_
#define CRPC_RPC_RPC_DISPATCHER_H_

#include "crpc/net/protocol/dispatcher.h"
#include "crpc/rpc/rpc_message.h"

#include <map>
#include <memory>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace crpc {

/**
 * RPC 请求分发器
 *
 * TcpConnection 解出 RpcMessage 后会交给 RpcDispatcher：
 *   1. 从 service_full_name 中拆出 Service 名和 Method 名
 *   2. 在已注册的 protobuf Service 表中查找目标服务
 *   3. 反序列化请求参数，调用 Service::CallMethod()
 *   4. 将返回值或错误信息重新编码到响应包
 */
class RpcDispatcher : public Dispatcher {
public:
	// using Ptr = std::shared_ptr<RpcDispatcher>;
	using ServicePtr = std::shared_ptr<google::protobuf::Service>;

	RpcDispatcher() = default;
	~RpcDispatcher() = default;

	// 处理一个完整的 RPC 请求包，并把响应写入 out_buffer
	void dispatch(ProtocolMessage* data, Codec* codec, TcpBuffer* out_buffer) override;

	// 将 "Service.Method" 拆分为 service_name 和 method_name
	bool parseServiceFullName(const std::string& full_name, std::string& service_name,
							  std::string& method_name);

	// 注册 protobuf 生成的 Service 实例，服务端启动阶段调用
	void registerService(ServicePtr service);

public:
	// 所有服务都应在进程启动前注册到这里
	// key 为 service 的 protobuf full_name，value 为业务 Service 实例
	std::map<std::string, ServicePtr> service_map_;
};

}  // namespace crpc

#endif

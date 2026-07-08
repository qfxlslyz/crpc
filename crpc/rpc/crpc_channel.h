#ifndef CRPC_RPC_CRPC_CHANNEL_H_
#define CRPC_RPC_CRPC_CHANNEL_H_

#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_client.h"

#include <memory>

#include <google/protobuf/service.h>

namespace crpc {

/**
 * RPC 客户端通道
 *
 * protobuf Stub 发起 RPC 时最终会调用 CrpcChannel::CallMethod()
 * 该类负责把 protobuf 请求对象封装成 RpcMessage，通过 TcpClient 发送，
 * 再把服务端返回的 pb_data 反序列化到 response
 */
class CrpcChannel : public google::protobuf::RpcChannel {
public:
	using Ptr = std::shared_ptr<CrpcChannel>;
	CrpcChannel(NetAddress::Ptr addr);
	~CrpcChannel() = default;

	// protobuf RpcChannel 标准入口，业务代码通常不会直接调用
	void CallMethod(const google::protobuf::MethodDescriptor* method,
					google::protobuf::RpcController* controller,
					const google::protobuf::Message* request, google::protobuf::Message* response,
					google::protobuf::Closure* done);

private:
	// 目标 RPC 服务端地址
	NetAddress::Ptr addr_;
	// TcpClient::Ptr client_;
};

}  // namespace crpc

#endif

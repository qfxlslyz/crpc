/**
 * 请求分发器接口
 * 负责将解码后的请求数据分发到对应的处理逻辑
 */
#ifndef CRPC_NET_PROTOCOL_DISPATCHER_H_
#define CRPC_NET_PROTOCOL_DISPATCHER_H_

#include "crpc/net/protocol/protocol_message.h"

#include <memory>

namespace crpc {

class Codec;
class TcpBuffer;

class Dispatcher {
public:
	using Ptr = std::shared_ptr<Dispatcher>;

	Dispatcher() {}

	virtual ~Dispatcher() {}

	// 将请求数据分发到具体的处理函数，处理结果写回 out_buffer
	virtual void dispatch(ProtocolMessage* data, Codec* codec, TcpBuffer* out_buffer) = 0;
};

}  // namespace crpc

#endif

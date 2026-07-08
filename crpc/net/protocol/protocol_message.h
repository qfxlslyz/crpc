/**
 * 协议消息基类
 * 各协议数据包的公共基类，标记编解码状态
 */
#ifndef CRPC_NET_PROTOCOL_PROTOCOL_MESSAGE_H_
#define CRPC_NET_PROTOCOL_PROTOCOL_MESSAGE_H_

#include <memory>

namespace crpc {

class ProtocolMessage {
public:
	using Ptr = std::shared_ptr<ProtocolMessage>;

	ProtocolMessage() = default;
	virtual ~ProtocolMessage(){};

	bool decode_succ{false};  // 解码是否成功
	bool encode_succ{false};  // 编码是否成功
};

}  // namespace crpc
#endif

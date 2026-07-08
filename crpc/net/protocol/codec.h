/**
 * 编解码器接口
 * 定义协议数据与字节流之间转换的接口
 */
#ifndef CRPC_NET_PROTOCOL_CODEC_H_
#define CRPC_NET_PROTOCOL_CODEC_H_

#include "crpc/net/protocol/protocol_message.h"
#include "crpc/net/transport/tcp_buffer.h"

#include <memory>
#include <string>

namespace crpc {

// 支持的应用层协议类型
enum ProtocolType {
	kDefaultRpcProtocol = 1	 // CRPC 默认二进制 RPC 编解码（基于 Protobuf）
};

class Codec {
public:
	using Ptr = std::shared_ptr<Codec>;

	Codec() {}

	virtual ~Codec() {}

	// 将结构化数据编码为字节流写入 TcpBuffer
	virtual void encode(TcpBuffer* buf, ProtocolMessage* data) = 0;

	// 从 TcpBuffer 中解码字节流为结构化数据
	virtual void decode(TcpBuffer* buf, ProtocolMessage* data) = 0;

	// 创建当前协议对应的数据包对象
	virtual ProtocolMessage::Ptr createData() = 0;

	// 提取请求序号，用于客户端将响应包与请求关联
	virtual std::string getMsgReq(ProtocolMessage* data) = 0;

	virtual ProtocolType getProtocolType() = 0;
};

}  // namespace crpc
#endif

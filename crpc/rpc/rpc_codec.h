#ifndef CRPC_RPC_RPC_CODEC_H_
#define CRPC_RPC_RPC_CODEC_H_

#include "crpc/net/protocol/codec.h"
#include "crpc/net/protocol/protocol_message.h"
#include "crpc/rpc/rpc_message.h"

#include <stdint.h>

namespace crpc {

/**
 * CRPC 默认二进制协议编解码器
 *
 * 包格式:
 *   PB_START | pk_len | msg_req_len | msg_req |
 *   service_full_name_len | service_full_name |
 *   err_code | err_info_len | err_info |
 *   pb_data | checksum | PB_END
 *
 * 其中 pb_data 是业务 protobuf 序列化后的字节串
 */
class RpcCodec : public Codec {
public:
	RpcCodec();

	~RpcCodec() override;

	void encode(TcpBuffer* buf, ProtocolMessage* data) override;

	void decode(TcpBuffer* buf, ProtocolMessage* data) override;

	ProtocolMessage::Ptr createData() override;

	std::string getMsgReq(ProtocolMessage* data) override;

	ProtocolType getProtocolType() override;

	// 将 RpcMessage 编码为一段连续内存，调用方负责释放返回的 malloc 内存
	const char* encodeRpcData(RpcMessage* data, int& len);
};

}  // namespace crpc

#endif

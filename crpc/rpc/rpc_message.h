#ifndef CRPC_RPC_RPC_MESSAGE_H_
#define CRPC_RPC_RPC_MESSAGE_H_

#include "crpc/base/log.h"
#include "crpc/net/protocol/protocol_message.h"

#include <stdint.h>
#include <string>
#include <vector>

namespace crpc {

class RpcMessage : public ProtocolMessage {
public:
	using Ptr = std::shared_ptr<RpcMessage>;
	RpcMessage() = default;
	~RpcMessage() = default;
	RpcMessage(const RpcMessage&) = default;
	RpcMessage& operator=(const RpcMessage&) = default;
	RpcMessage(RpcMessage&&) = default;
	RpcMessage& operator=(RpcMessage&&) = default;

	/*
	**  最小包长度为：1 + 4 + 4 + 4 + 4 + 4 + 4 + 1 = 26 字节
	**  该对象只保存协议字段，不负责网络字节序转换；转换逻辑在 RpcCodec 中完成
	*/

	// char Start;                      // 标识 RPC 数据的起始位置
	int32_t pk_len{0};				// 整个数据包长度（包含起始和结束字符）
	int32_t msg_req_len{0};			// msg_req 长度
	std::string msg_req;			// msg_req，用于标识一次请求
	int32_t service_name_len{0};	// service 完整名称长度
	std::string service_full_name;	// service 完整名称，例如 QueryService.query_name
	int32_t err_code{0};	  // err_code，0 表示 RPC 调用成功，非 0 表示失败；只由
							  // CrpcController 设置
	int32_t err_info_len{0};  // err_info 长度
	std::string err_info;	  // err_info，空字符串表示 RPC
						   // 调用成功，否则保存失败原因详情；只由 CrpcController 设置
	std::string pb_data;	// 业务 Protobuf 数据
	int32_t check_num{-1};	// 整个包的校验值，用于检查数据合法性
							// char end;                        // 标识 RPC 数据的结束位置
};

}  // namespace crpc
#endif

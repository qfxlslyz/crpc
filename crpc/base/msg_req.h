/**
 * 请求 ID 生成工具
 * 为每次 RPC 调用生成请求标识号，用于链路追踪和日志关联
 */
#ifndef CRPC_BASE_MSG_REQ_H_
#define CRPC_BASE_MSG_REQ_H_

#include <string>

namespace crpc {

class MsgReqUtil {
public:
	// 生成线程内递增的随机数字请求 ID
	static std::string genMsgNumber();
};

}  // namespace crpc

#endif

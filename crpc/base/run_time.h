/**
 * 协程运行时上下文
 * 保存当前协程正在处理的 RPC 请求信息，便于在日志中输出请求 ID 和接口名
 * 每个协程持有自己的 RunTime 实例
 */
#ifndef CRPC_BASE_RUN_TIME_H_
#define CRPC_BASE_RUN_TIME_H_

#include <string>

namespace crpc {

class RunTime {
public:
	std::string msg_no_;		  // 当前请求的唯一标识号
	std::string interface_name_;  // 当前正在处理的 RPC 接口名（如
								  // "OrderService.queryOrder"）
};

}  // namespace crpc

#endif
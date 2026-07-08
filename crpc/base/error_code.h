/**
 * 错误码定义
 * 所有 RPC 框架内部的错误码统一以 1000 为前缀，便于与业务错误码区分
 */
#ifndef CRPC_BASE_ERROR_CODE_H_
#define CRPC_BASE_ERROR_CODE_H_

namespace crpc {

// 系统错误码前缀宏: SYS_ERROR_PREFIX(0001) => 10000001
#ifndef SYS_ERROR_PREFIX
#define SYS_ERROR_PREFIX(xx) 1000##xx
#endif	// SYS_ERROR_PREFIX(xx)

const int ERROR_PEER_CLOSED = SYS_ERROR_PREFIX(0000);		  // 对端关闭连接
const int ERROR_FAILED_CONNECT = SYS_ERROR_PREFIX(0001);	  // 连接对端主机失败
const int ERROR_FAILED_GET_REPLY = SYS_ERROR_PREFIX(0002);	  // 获取服务端响应失败
const int ERROR_FAILED_DESERIALIZE = SYS_ERROR_PREFIX(0003);  // 反序列化失败
const int ERROR_FAILED_SERIALIZE = SYS_ERROR_PREFIX(0004);	  // 序列化失败

const int ERROR_FAILED_ENCODE = SYS_ERROR_PREFIX(0005);	 // 编码失败
const int ERROR_FAILED_DECODE = SYS_ERROR_PREFIX(0006);	 // 解码失败

const int ERROR_RPC_CALL_TIMEOUT = SYS_ERROR_PREFIX(0007);	// RPC 调用超时

const int ERROR_SERVICE_NOT_FOUND = SYS_ERROR_PREFIX(0008);	 // 未找到对应的 Service

const int ERROR_METHOD_NOT_FOUND = SYS_ERROR_PREFIX(0009);	// 未找到对应的 Method

const int ERROR_PARSE_SERVICE_NAME = SYS_ERROR_PREFIX(0010);	  // 解析 service name 失败
const int ERROR_NOT_SET_ASYNC_PRE_CALL = SYS_ERROR_PREFIX(0011);  // 异步 RPC 调用缺少必要参数
const int ERROR_CONNECT_SYS_ERR = SYS_ERROR_PREFIX(0012);		  // 连接时发生系统错误

}  // namespace crpc

#endif	// CRPC_COMM_ERRORCODE_H

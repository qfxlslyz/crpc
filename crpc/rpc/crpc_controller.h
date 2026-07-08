#ifndef CRPC_RPC_CRPC_CONTROLLER_H_
#define CRPC_RPC_CRPC_CONTROLLER_H_

#include "crpc/net/transport/net_address.h"

#include <memory>
#include <stdio.h>

#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>

namespace crpc {

/**
 * RPC 调用控制器
 *
 * protobuf 的 RpcController 只定义了通用控制接口，CRPC 在此扩展了：
 *   - 错误码和错误信息
 *   - 请求序号 msg_req
 *   - 本端/对端地址
 *   - 调用超时时间
 *   - 当前方法名
 */
class CrpcController : public google::protobuf::RpcController {
public:
	using Ptr = std::shared_ptr<CrpcController>;
	// ===== protobuf RpcController 标准接口 =====

	CrpcController() = default;

	~CrpcController() = default;

	void Reset() override;

	bool Failed() const override;

	std::string ErrorText() const override;

	void StartCancel() override;

	void SetFailed(const std::string& reason) override;

	bool IsCanceled() const override;

	void NotifyOnCancel(google::protobuf::Closure* callback) override;

	// ===== CRPC 扩展接口 =====

	int errorCode() const;

	void setErrorCode(const int error_code);

	const std::string& msgSeq() const;

	void setMsgReq(const std::string& msg_req);

	void setError(const int err_code, const std::string& err_info);

	// 记录一次调用涉及的地址，便于业务和日志获取上下文
	void setPeerAddr(NetAddress::Ptr addr);

	void setLocalAddr(NetAddress::Ptr addr);

	NetAddress::Ptr peerAddr();

	NetAddress::Ptr localAddr();

	void setTimeout(const int timeout);

	int timeout() const;

	void setMethodName(const std::string& name);

	std::string getMethodName();

	void setMethodFullName(const std::string& name);

	std::string getMethodFullName();

private:
	int error_code_{0};		  // 错误码，标识一个具体错误
	std::string error_info_;  // 错误信息，描述错误详情
	std::string msg_req_;	  // msg_req，用于标识一次 RPC 请求和响应
	bool is_failed_{false};
	bool is_canceled_{false};
	NetAddress::Ptr peer_addr_;
	NetAddress::Ptr local_addr_;

	int timeout_{5000};		   // RPC 调用最大超时时间
	std::string method_name_;  // 方法名
	std::string full_name_;	   // 完整方法名，例如 server.method_name
};

}  // namespace crpc

#endif

#include "crpc/base/error_code.h"
#include "crpc/base/log.h"
#include "crpc/base/msg_req.h"
#include "crpc/base/run_time.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_client.h"
#include "crpc/rpc/crpc_channel.h"
#include "crpc/rpc/crpc_controller.h"
#include "crpc/rpc/rpc_codec.h"
#include "crpc/rpc/rpc_message.h"

#include <memory>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace crpc {

CrpcChannel::CrpcChannel(NetAddress::Ptr addr) : addr_(addr) {}

void CrpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
							 google::protobuf::RpcController* controller,
							 const google::protobuf::Message* request,
							 google::protobuf::Message* response, google::protobuf::Closure* done) {
	RpcMessage pb_struct;
	// CRPC 依赖自定义 Controller 保存超时、错误码、地址和 msg_req 等上下文
	CrpcController* rpc_controller = dynamic_cast<CrpcController*>(controller);
	if (!rpc_controller) {
		ErrorLog << "call failed. failed to dynamic cast CrpcController";
		return;
	}

	// 每次 RPC 调用创建一个短连接客户端，发送请求并同步等待响应
	Codec::Ptr client_codec = std::make_shared<RpcCodec>();
	TcpClient::Ptr client_ = std::make_shared<TcpClient>(addr_, client_codec, kDefaultRpcProtocol);
	rpc_controller->setLocalAddr(client_->getLocalAddr());
	rpc_controller->setPeerAddr(client_->getPeerAddr());

	pb_struct.service_full_name = method->full_name();
	DebugLog << "call service_name = " << pb_struct.service_full_name;
	// 请求体只存放业务 protobuf 的序列化结果，服务名和错误码属于 RPC 外层协议
	if (!request->SerializeToString(&(pb_struct.pb_data))) {
		ErrorLog << "serialize send package error";
		return;
	}

	if (!rpc_controller->msgSeq().empty()) {
		pb_struct.msg_req = rpc_controller->msgSeq();
	} else {
		// 如果是在一次服务端 RPC 处理过程中发起下游调用，复用当前协程的 msg_req
		// 形成调用链
		RunTime* run_time = GetCurrentRunTime();
		if (run_time != nullptr && !run_time->msg_no_.empty()) {
			pb_struct.msg_req = run_time->msg_no_;
			DebugLog << "get from RunTime succ, msgno = " << pb_struct.msg_req;
		} else {
			pb_struct.msg_req = MsgReqUtil::genMsgNumber();
			DebugLog << "get from RunTime error, generate new msgno = " << pb_struct.msg_req;
		}
		rpc_controller->setMsgReq(pb_struct.msg_req);
	}

	// 先编码到 TcpConnection 的写缓冲区，sendAndRecv 内部负责真正写 socket
	Codec::Ptr codec_ = client_->getConnection()->getCodec();
	codec_->encode(client_->getConnection()->getOutBuffer(), &pb_struct);
	if (!pb_struct.encode_succ) {
		rpc_controller->setError(ERROR_FAILED_ENCODE, "encode rpc data error");
		return;
	}

	InfoLog << "============================================================";
	InfoLog << pb_struct.msg_req << "|" << rpc_controller->peerAddr()->toString()
			<< "|. Set client send request data:" << request->ShortDebugString();
	InfoLog << "============================================================";
	client_->setTimeout(rpc_controller->timeout());

	// sendAndRecv 会完成连接、发送、读取响应和超时处理
	ProtocolMessage::Ptr raw_res_data;
	int rt = client_->sendAndRecv(pb_struct.msg_req, raw_res_data);
	if (rt != 0) {
		rpc_controller->setError(rt, client_->getErrInfo());
		ErrorLog << pb_struct.msg_req << "|call rpc occur client error, service_full_name="
				 << pb_struct.service_full_name << ", error_code=" << rt
				 << ", error_info = " << client_->getErrInfo();
		return;
	}
	RpcMessage::Ptr res_data = std::dynamic_pointer_cast<RpcMessage>(raw_res_data);
	if (!res_data) {
		rpc_controller->setError(ERROR_FAILED_DESERIALIZE,
								 "failed to cast response data to RpcMessage");
		ErrorLog << pb_struct.msg_req << "|failed to cast response data to RpcMessage";
		return;
	}

	// 先反序列化业务响应，再检查 RPC 层错误码，便于统一返回错误上下文
	if (!response->ParseFromString(res_data->pb_data)) {
		rpc_controller->setError(ERROR_FAILED_DESERIALIZE,
								 "failed to deserialize data from server");
		ErrorLog << pb_struct.msg_req << "|failed to deserialize data";
		return;
	}
	if (res_data->err_code != 0) {
		ErrorLog << pb_struct.msg_req << "|server reply error_code=" << res_data->err_code
				 << ", err_info=" << res_data->err_info;
		rpc_controller->setError(res_data->err_code, res_data->err_info);
		return;
	}

	InfoLog << "============================================================";
	InfoLog << pb_struct.msg_req << "|" << rpc_controller->peerAddr()->toString()
			<< "|call rpc server [" << pb_struct.service_full_name << "] succ"
			<< ". Get server reply response data:" << response->ShortDebugString();
	InfoLog << "============================================================";

	// 执行回调函数
	if (done) {
		done->Run();
	}
}

}  // namespace crpc

#include "crpc/base/error_code.h"
#include "crpc/base/msg_req.h"
#include "crpc/net/protocol/codec.h"
#include "crpc/net/protocol/dispatcher.h"
#include "crpc/net/transport/tcp_buffer.h"
#include "crpc/rpc/rpc_closure.h"
#include "crpc/rpc/rpc_codec.h"
#include "crpc/rpc/crpc_controller.h"
#include "crpc/rpc/rpc_dispatcher.h"
#include "crpc/rpc/rpc_message.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace crpc {

void RpcDispatcher::dispatch(ProtocolMessage* data, Codec* codec, TcpBuffer* out_buffer) {
	// 网络层只认识 ProtocolMessage，这里转换为 RPC 协议包继续处理
	RpcMessage* tmp = dynamic_cast<RpcMessage*>(data);

	if (tmp == nullptr) {
		ErrorLog << "dynamic_cast error";
		return;
	}

	// 将请求号写入当前协程运行时，后续日志、子 RPC 调用会沿用同一个 msg_req
	Coroutine::getCurrentCoroutine()->getRunTime()->msg_no_ = tmp->msg_req;
	SetCurrentRunTime(Coroutine::getCurrentCoroutine()->getRunTime());

	InfoLog << "begin to dispatch client rpc request, msgno=" << tmp->msg_req;

	std::string service_name;
	std::string method_name;

	RpcMessage reply_pk;
	reply_pk.service_full_name = tmp->service_full_name;
	reply_pk.msg_req = tmp->msg_req;
	if (reply_pk.msg_req.empty()) {
		// 客户端未传 msg_req 时由服务端兜底生成，保证响应仍可被追踪
		reply_pk.msg_req = MsgReqUtil::genMsgNumber();
	}

	// service_full_name 采用 protobuf 的 MethodDescriptor::full_name() 格式
	if (!parseServiceFullName(tmp->service_full_name, service_name, method_name)) {
		ErrorLog << reply_pk.msg_req << "|parse service name " << tmp->service_full_name << "error";

		reply_pk.err_code = ERROR_PARSE_SERVICE_NAME;
		std::stringstream ss;
		ss << "cannot parse service_name:[" << tmp->service_full_name << "]";
		reply_pk.err_info = ss.str();
		codec->encode(out_buffer, dynamic_cast<ProtocolMessage*>(&reply_pk));
		return;
	}

	Coroutine::getCurrentCoroutine()->getRunTime()->interface_name_ = tmp->service_full_name;
	// 先通过 service 名定位到具体的 protobuf Service 实例
	auto it = service_map_.find(service_name);
	if (it == service_map_.end() || !((*it).second)) {
		reply_pk.err_code = ERROR_SERVICE_NOT_FOUND;
		std::stringstream ss;
		ss << "not found service_name:[" << service_name << "]";
		ErrorLog << reply_pk.msg_req << "|" << ss.str();
		reply_pk.err_info = ss.str();

		codec->encode(out_buffer, dynamic_cast<ProtocolMessage*>(&reply_pk));

		InfoLog << "end dispatch client rpc request, msgno=" << tmp->msg_req;
		return;
	}

	ServicePtr service = (*it).second;

	// 再通过 method 名从 protobuf 描述信息中定位具体 RPC 方法
	const google::protobuf::MethodDescriptor* method =
		service->GetDescriptor()->FindMethodByName(method_name);
	if (!method) {
		reply_pk.err_code = ERROR_METHOD_NOT_FOUND;
		std::stringstream ss;
		ss << "not found method_name:[" << method_name << "]";
		ErrorLog << reply_pk.msg_req << "|" << ss.str();
		reply_pk.err_info = ss.str();
		codec->encode(out_buffer, dynamic_cast<ProtocolMessage*>(&reply_pk));
		return;
	}

	// 根据方法描述创建请求对象，实际类型由 protobuf 生成代码提供
	google::protobuf::Message* request = service->GetRequestPrototype(method).New();
	DebugLog << reply_pk.msg_req << "|request.name = " << request->GetDescriptor()->full_name();

	if (!request->ParseFromString(tmp->pb_data)) {
		reply_pk.err_code = ERROR_FAILED_SERIALIZE;
		std::stringstream ss;
		ss << "failed to parse request data, request.name:["
		   << request->GetDescriptor()->full_name() << "]";
		reply_pk.err_info = ss.str();
		ErrorLog << reply_pk.msg_req << "|" << ss.str();
		delete request;
		codec->encode(out_buffer, dynamic_cast<ProtocolMessage*>(&reply_pk));
		return;
	}

	InfoLog << "============================================================";
	InfoLog << reply_pk.msg_req << "|Get client request data:" << request->ShortDebugString();
	InfoLog << "============================================================";

	// 响应对象同样通过 protobuf 原型创建，避免分发器依赖业务类型
	google::protobuf::Message* response = service->GetResponsePrototype(method).New();

	DebugLog << reply_pk.msg_req << "|response.name = " << response->GetDescriptor()->full_name();

	CrpcController rpc_controller;
	rpc_controller.setMsgReq(reply_pk.msg_req);
	rpc_controller.setMethodName(method_name);
	rpc_controller.setMethodFullName(tmp->service_full_name);

	std::function<void()> reply_package_func = []() {};

	// 当前框架使用同步式调用，closure 作为 protobuf 接口要求的占位回调
	RpcClosure closure(reply_package_func);
	service->CallMethod(method, &rpc_controller, request, response, &closure);

	InfoLog << "Call [" << reply_pk.service_full_name << "] succ, now send reply package";

	if (!(response->SerializeToString(&(reply_pk.pb_data)))) {
		reply_pk.pb_data = "";
		ErrorLog << reply_pk.msg_req << "|reply error! encode reply package error";
		reply_pk.err_code = ERROR_FAILED_SERIALIZE;
		reply_pk.err_info = "failed to serilize relpy data";
	} else {
		InfoLog << "============================================================";
		InfoLog << reply_pk.msg_req << "|Set server response data:" << response->ShortDebugString();
		InfoLog << "============================================================";
	}

	delete request;
	delete response;

	// 将响应包编码到连接的写缓冲区，由 TcpConnection::output() 发送
	codec->encode(out_buffer, dynamic_cast<ProtocolMessage*>(&reply_pk));
}

bool RpcDispatcher::parseServiceFullName(const std::string& full_name, std::string& service_name,
										 std::string& method_name) {
	if (full_name.empty()) {
		ErrorLog << "service_full_name empty";
		return false;
	}
	std::size_t i = full_name.find(".");
	if (i == full_name.npos) {
		ErrorLog << "not found [.]";
		return false;
	}

	// 约定只按第一个 '.' 切分，左侧为 Service，右侧为 Method
	service_name = full_name.substr(0, i);
	DebugLog << "service_name = " << service_name;
	method_name = full_name.substr(i + 1, full_name.length() - i - 1);
	DebugLog << "method_name = " << method_name;

	return true;
}

void RpcDispatcher::registerService(ServicePtr service) {
	std::string service_name = service->GetDescriptor()->full_name();
	service_map_[service_name] = service;
	InfoLog << "succ register service[" << service_name << "]!";
}

}  // namespace crpc

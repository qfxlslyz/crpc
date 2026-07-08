#include "crpc/rpc/crpc_controller.h"

#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>

namespace crpc {

void CrpcController::Reset() {}

bool CrpcController::Failed() const {
	return is_failed_;
}

std::string CrpcController::ErrorText() const {
	return error_info_;
}

void CrpcController::StartCancel() {}

void CrpcController::SetFailed(const std::string& reason) {
	is_failed_ = true;
	error_info_ = reason;
}

bool CrpcController::IsCanceled() const {
	return false;
}

void CrpcController::NotifyOnCancel(google::protobuf::Closure* callback) {}

void CrpcController::setErrorCode(const int error_code) {
	error_code_ = error_code;
}

int CrpcController::errorCode() const {
	return error_code_;
}

const std::string& CrpcController::msgSeq() const {
	return msg_req_;
}

void CrpcController::setMsgReq(const std::string& msg_req) {
	msg_req_ = msg_req;
}

void CrpcController::setError(const int err_code, const std::string& err_info) {
	SetFailed(err_info);
	setErrorCode(err_code);
}

void CrpcController::setPeerAddr(NetAddress::Ptr addr) {
	peer_addr_ = addr;
}

void CrpcController::setLocalAddr(NetAddress::Ptr addr) {
	local_addr_ = addr;
}
NetAddress::Ptr CrpcController::peerAddr() {
	return peer_addr_;
}

NetAddress::Ptr CrpcController::localAddr() {
	return local_addr_;
}

void CrpcController::setTimeout(const int timeout) {
	timeout_ = timeout;
}
int CrpcController::timeout() const {
	return timeout_;
}

void CrpcController::setMethodName(const std::string& name) {
	method_name_ = name;
}

std::string CrpcController::getMethodName() {
	return method_name_;
}

void CrpcController::setMethodFullName(const std::string& name) {
	full_name_ = name;
}

std::string CrpcController::getMethodFullName() {
	return full_name_;
}

}  // namespace crpc

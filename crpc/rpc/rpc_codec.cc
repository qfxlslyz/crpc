#include "crpc/base/log.h"
#include "crpc/base/msg_req.h"
#include "crpc/net/protocol/byte.h"
#include "crpc/net/protocol/protocol_message.h"
#include "crpc/rpc/rpc_codec.h"
#include "crpc/rpc/rpc_message.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string.h>
#include <vector>

namespace crpc {

static const char PB_START = 0x02;	// 起始标志字符
static const char PB_END = 0x03;	// 结束标志字符
static const int MSG_REQ_LEN = 20;	// msg_req 默认长度

RpcCodec::RpcCodec() {}

RpcCodec::~RpcCodec() {}

ProtocolMessage::Ptr RpcCodec::createData() {
	return std::make_shared<RpcMessage>();
}

std::string RpcCodec::getMsgReq(ProtocolMessage* data) {
	RpcMessage* tmp = dynamic_cast<RpcMessage*>(data);
	if (!tmp) {
		return "";
	}
	return tmp->msg_req;
}

void RpcCodec::encode(TcpBuffer* buf, ProtocolMessage* data) {
	if (!buf || !data) {
		ErrorLog << "encode error! buf or data nullptr";
		return;
	}
	// 这里必须是 RpcMessage，否则无法按 CRPC 协议字段编码
	RpcMessage* tmp = dynamic_cast<RpcMessage*>(data);

	int len = 0;
	const char* re = encodeRpcData(tmp, len);
	if (re == nullptr || len == 0 || !tmp->encode_succ) {
		ErrorLog << "encode error";
		data->encode_succ = false;
		return;
	}
	DebugLog << "encode package len = " << len;
	if (buf != nullptr) {
		buf->writeToBuffer(re, len);
		DebugLog << "succ encode and write to buffer, writeindex=" << buf->writeIndex();
	}
	data = tmp;
	if (re) {
		free((void*)re);
		re = nullptr;
	}
}

const char* RpcCodec::encodeRpcData(RpcMessage* data, int& len) {
	if (data->service_full_name.empty()) {
		ErrorLog << "parse error, service_full_name is empty";
		data->encode_succ = false;
		return nullptr;
	}
	if (data->msg_req.empty()) {
		data->msg_req = MsgReqUtil::genMsgNumber();
		data->msg_req_len = data->msg_req.length();
		DebugLog << "generate msgno = " << data->msg_req;
	}

	// 计算完整包长，长度字段本身也包含在包内
	int32_t pk_len = 2 * sizeof(char) + 6 * sizeof(int32_t) + data->pb_data.length() +
					 data->service_full_name.length() + data->msg_req.length() +
					 data->err_info.length();

	DebugLog << "encode pk_len = " << pk_len;
	char* buf = reinterpret_cast<char*>(malloc(pk_len));
	char* tmp = buf;
	*tmp = PB_START;
	tmp++;

	// 所有整数字段统一使用网络字节序，避免不同机器字节序不一致
	int32_t pk_len_net = htonl(pk_len);
	memcpy(tmp, &pk_len_net, sizeof(int32_t));
	tmp += sizeof(int32_t);

	int32_t msg_req_len = data->msg_req.length();
	DebugLog << "msg_req_len= " << msg_req_len;
	int32_t msg_req_len_net = htonl(msg_req_len);
	memcpy(tmp, &msg_req_len_net, sizeof(int32_t));
	tmp += sizeof(int32_t);

	if (msg_req_len != 0) {
		memcpy(tmp, &(data->msg_req[0]), msg_req_len);
		tmp += msg_req_len;
	}

	int32_t service_full_name_len = data->service_full_name.length();
	DebugLog << "src service_full_name_len = " << service_full_name_len;
	int32_t service_full_name_len_net = htonl(service_full_name_len);
	memcpy(tmp, &service_full_name_len_net, sizeof(int32_t));
	tmp += sizeof(int32_t);

	if (service_full_name_len != 0) {
		memcpy(tmp, &(data->service_full_name[0]), service_full_name_len);
		tmp += service_full_name_len;
	}

	int32_t err_code = data->err_code;
	DebugLog << "err_code= " << err_code;
	int32_t err_code_net = htonl(err_code);
	memcpy(tmp, &err_code_net, sizeof(int32_t));
	tmp += sizeof(int32_t);

	int32_t err_info_len = data->err_info.length();
	DebugLog << "err_info_len= " << err_info_len;
	int32_t err_info_len_net = htonl(err_info_len);
	memcpy(tmp, &err_info_len_net, sizeof(int32_t));
	tmp += sizeof(int32_t);

	if (err_info_len != 0) {
		memcpy(tmp, &(data->err_info[0]), err_info_len);
		tmp += err_info_len;
	}

	memcpy(tmp, &(data->pb_data[0]), data->pb_data.length());
	tmp += data->pb_data.length();
	DebugLog << "pb_data_len= " << data->pb_data.length();

	int32_t checksum = 1;
	int32_t checksum_net = htonl(checksum);
	memcpy(tmp, &checksum_net, sizeof(int32_t));
	tmp += sizeof(int32_t);

	*tmp = PB_END;

	data->pk_len = pk_len;
	data->msg_req_len = msg_req_len;
	data->service_name_len = service_full_name_len;
	data->err_info_len = err_info_len;

	// 校验和字段预留，当前版本暂未实现真实校验
	data->check_num = checksum;
	data->encode_succ = true;

	len = pk_len;

	return buf;
}

void RpcCodec::decode(TcpBuffer* buf, ProtocolMessage* data) {
	if (!buf || !data) {
		ErrorLog << "decode error! buf or data nullptr";
		return;
	}

	std::vector<char> tmp = buf->getBufferVector();
	// int total_size = buf->readAble();
	int start_index = buf->readIndex();
	int end_index = -1;
	int32_t pk_len = -1;

	bool parse_full_pack = false;

	// TcpBuffer 中可能存在半包、粘包或无效字节，需要先定位完整的
	// PB_START/PB_END 包
	for (int i = start_index; i < buf->writeIndex(); ++i) {
		// 先查找起始标志
		if (tmp[i] == PB_START) {
			if (i + 1 < buf->writeIndex()) {
				pk_len = GetInt32FromNetByte(&tmp[i + 1]);
				DebugLog << "prase pk_len =" << pk_len;
				int j = i + pk_len - 1;
				DebugLog << "j =" << j << ", i=" << i;

				if (j >= buf->writeIndex()) {
					continue;
				}
				if (tmp[j] == PB_END) {
					start_index = i;
					end_index = j;
					parse_full_pack = true;
					break;
				}
			}
		}
	}

	if (!parse_full_pack) {
		// 数据还没收完整时保留缓冲区内容，等待下一次读事件继续解析
		DebugLog << "not parse full package, return";
		return;
	}

	buf->recycleRead(end_index + 1 - start_index);

	DebugLog << "read_buffer_ size=" << buf->getBufferVector().size() << "rd=" << buf->readIndex()
			 << "wd=" << buf->writeIndex();

	// 从完整包中按协议顺序解析各个字段
	RpcMessage* pb_struct = dynamic_cast<RpcMessage*>(data);
	pb_struct->pk_len = pk_len;
	pb_struct->decode_succ = false;

	int msg_req_len_index = start_index + sizeof(char) + sizeof(int32_t);
	if (msg_req_len_index >= end_index) {
		ErrorLog << "parse error, msg_req_len_index[" << msg_req_len_index << "] >= end_index["
				 << end_index << "]";
		// 丢弃这个错误数据包
		return;
	}

	pb_struct->msg_req_len = GetInt32FromNetByte(&tmp[msg_req_len_index]);
	if (pb_struct->msg_req_len == 0) {
		ErrorLog << "prase error, msg_req emptr";
		return;
	}

	DebugLog << "msg_req_len= " << pb_struct->msg_req_len;
	int msg_req_index = msg_req_len_index + sizeof(int32_t);
	DebugLog << "msg_req_len_index= " << msg_req_index;

	char msg_req[50] = {0};

	memcpy(&msg_req[0], &tmp[msg_req_index], pb_struct->msg_req_len);
	pb_struct->msg_req = std::string(msg_req);
	DebugLog << "msg_req= " << pb_struct->msg_req;

	int service_name_len_index = msg_req_index + pb_struct->msg_req_len;
	if (service_name_len_index >= end_index) {
		ErrorLog << "parse error, service_name_len_index[" << service_name_len_index
				 << "] >= end_index[" << end_index << "]";
		// 丢弃这个错误数据包
		return;
	}

	DebugLog << "service_name_len_index = " << service_name_len_index;
	int service_name_index = service_name_len_index + sizeof(int32_t);

	if (service_name_index >= end_index) {
		ErrorLog << "parse error, service_name_index[" << service_name_index << "] >= end_index["
				 << end_index << "]";
		return;
	}

	pb_struct->service_name_len = GetInt32FromNetByte(&tmp[service_name_len_index]);

	if (pb_struct->service_name_len > pk_len) {
		ErrorLog << "parse error, service_name_len[" << pb_struct->service_name_len
				 << "] >= pk_len [" << pk_len << "]";
		return;
	}
	DebugLog << "service_name_len = " << pb_struct->service_name_len;

	char service_name[512] = {0};

	memcpy(&service_name[0], &tmp[service_name_index], pb_struct->service_name_len);
	pb_struct->service_full_name = std::string(service_name);
	DebugLog << "service_name = " << pb_struct->service_full_name;

	int err_code_index = service_name_index + pb_struct->service_name_len;
	pb_struct->err_code = GetInt32FromNetByte(&tmp[err_code_index]);

	int err_info_len_index = err_code_index + sizeof(int32_t);

	if (err_info_len_index >= end_index) {
		ErrorLog << "parse error, err_info_len_index[" << err_info_len_index << "] >= end_index["
				 << end_index << "]";
		// 丢弃这个错误数据包
		return;
	}
	pb_struct->err_info_len = GetInt32FromNetByte(&tmp[err_info_len_index]);
	DebugLog << "err_info_len = " << pb_struct->err_info_len;
	int err_info_index = err_info_len_index + sizeof(int32_t);

	char err_info[512] = {0};

	memcpy(&err_info[0], &tmp[err_info_index], pb_struct->err_info_len);
	pb_struct->err_info = std::string(err_info);

	int pb_data_len = pb_struct->pk_len - pb_struct->service_name_len - pb_struct->msg_req_len -
					  pb_struct->err_info_len - 2 * sizeof(char) - 6 * sizeof(int32_t);

	int pb_data_index = err_info_index + pb_struct->err_info_len;
	DebugLog << "pb_data_len= " << pb_data_len << ", pb_index = " << pb_data_index;

	if (pb_data_index >= end_index) {
		ErrorLog << "parse error, pb_data_index[" << pb_data_index << "] >= end_index[" << end_index
				 << "]";
		return;
	}

	// pb_data 允许包含 '\0' 字节，必须按长度构造 string，不能依赖 C
	// 字符串结尾
	std::string pb_data_str(&tmp[pb_data_index], pb_data_len);
	pb_struct->pb_data = pb_data_str;

	pb_struct->decode_succ = true;
	data = pb_struct;
}

ProtocolType RpcCodec::getProtocolType() {
	return kDefaultRpcProtocol;
}

}  // namespace crpc

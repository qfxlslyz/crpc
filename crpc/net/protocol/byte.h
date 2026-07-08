/**
 * 字节序转换工具
 * 网络字节序（大端）与主机字节序之间的转换
 */
#ifndef CRPC_NET_PROTOCOL_BYTE_H_
#define CRPC_NET_PROTOCOL_BYTE_H_

#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>

namespace crpc {

// 从网络字节序的缓冲区中读取一个 int32，转换为主机字节序
int32_t GetInt32FromNetByte(const char* buf) {
	int32_t tmp;
	memcpy(&tmp, buf, sizeof(tmp));
	return ntohl(tmp);
}

}  // namespace crpc
#endif

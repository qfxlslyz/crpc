/**
 * 网络地址封装
 * 提供 IP 地址（IPv4）和 Unix Domain Socket 地址的统一抽象
 */
#ifndef CRPC_NET_TRANSPORT_NET_ADDRESS_H_
#define CRPC_NET_TRANSPORT_NET_ADDRESS_H_

#include <arpa/inet.h>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

namespace crpc {

class NetAddress {
public:
	using Ptr = std::shared_ptr<NetAddress>;

	virtual sockaddr* getSockAddr() = 0;

	virtual int getFamily() const = 0;

	virtual std::string toString() const = 0;

	virtual socklen_t getSockLen() const = 0;
};

class IPAddress : public NetAddress {
public:
	IPAddress(const std::string& ip, uint16_t port);

	IPAddress(const std::string& addr);

	IPAddress(uint16_t port);

	IPAddress(sockaddr_in addr);

	sockaddr* getSockAddr() override;

	int getFamily() const override;

	socklen_t getSockLen() const override;

	std::string toString() const override;

	std::string getIP() const { return ip_; }

	int getPort() const { return port_; }

public:
	static bool checkValidIPAddr(const std::string& addr);

private:
	std::string ip_;
	uint16_t port_;
	sockaddr_in addr_;
};

class UnixDomainAddress : public NetAddress {
public:
	UnixDomainAddress(std::string& path);

	UnixDomainAddress(sockaddr_un addr);

	sockaddr* getSockAddr() override;

	int getFamily() const override;

	socklen_t getSockLen() const override;

	std::string getPath() const { return path_; }

	std::string toString() const override;

private:
	std::string path_;
	sockaddr_un addr_;
};

}  // namespace crpc

#endif

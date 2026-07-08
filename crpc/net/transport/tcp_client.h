#ifndef CRPC_NET_TRANSPORT_TCP_CLIENT_H_
#define CRPC_NET_TRANSPORT_TCP_CLIENT_H_

#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/protocol/codec.h"
#include "crpc/net/protocol/protocol_message.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_connection.h"

#include <memory>

namespace crpc {

/**
 * 同步风格的 TCP 客户端
 *
 * 应在协程中使用 TcpClient（不要在主协程中使用）。connect/read/write 会被
 * hook， 在 IO 未就绪时让出当前协程，从调用者视角看仍是顺序执行的 sendAndRecv
 */
class TcpClient {
public:
	using Ptr = std::shared_ptr<TcpClient>;

	TcpClient(NetAddress::Ptr addr, Codec::Ptr codec, ProtocolType type = kDefaultRpcProtocol);

	~TcpClient();

	void init();

	void resetFd();

	// 发送当前连接写缓冲区中的请求，并等待 msg_no 对应的响应包
	int sendAndRecv(const std::string& msg_no, ProtocolMessage::Ptr& res);

	void stop();

	TcpConnection* getConnection();

	void setTimeout(const int v) { max_timeout_ = v; }

	void setTryCounts(const int v) { try_counts_ = v; }

	const std::string& getErrInfo() { return err_info_; }

	NetAddress::Ptr getPeerAddr() const { return peer_addr_; }

	NetAddress::Ptr getLocalAddr() const { return local_addr_; }

	Codec::Ptr getCodec() { return codec_; }

private:
	int family_{0};
	int fd_{-1};
	int try_counts_{3};		  // 最大重连尝试次数
	int max_timeout_{10000};  // 最大调用超时时间，单位毫秒
	bool is_stop_{false};
	std::string err_info_;	// 客户端错误信息

	NetAddress::Ptr local_addr_{nullptr};
	NetAddress::Ptr peer_addr_{nullptr};
	Reactor* reactor_{nullptr};
	TcpConnection::Ptr connection_{nullptr};  // 客户端侧连接对象，持有读写缓冲区

	Codec::Ptr codec_{nullptr};	 // 与服务端一致的协议编解码器

	bool connect_succ_{false};
};

}  // namespace crpc

#endif

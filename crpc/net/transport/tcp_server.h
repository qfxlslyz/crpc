/**
 * TCP 服务器
 * CRPC 的核心服务端组件，负责:
 *   1. 监听端口，通过 TcpAcceptor 接受新连接
 *   2. 将新连接分配给 IO 线程池中的某个线程处理
 *   3. 创建 TcpConnection 管理每个客户端连接
 *   4. 通过时间轮定期清理超时的空闲连接
 */
#ifndef CRPC_NET_TRANSPORT_TCP_SERVER_H_
#define CRPC_NET_TRANSPORT_TCP_SERVER_H_

#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/event/timer.h"
#include "crpc/net/protocol/codec.h"
#include "crpc/net/protocol/dispatcher.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"

#include <map>

namespace crpc {

// 连接接收器：封装 listen socket 的 bind/listen/accept 操作
class TcpAcceptor {
public:
	using Ptr = std::shared_ptr<TcpAcceptor>;
	TcpAcceptor(NetAddress::Ptr net_addr);

	void init();

	// 阻塞式 accept，返回新连接的 fd（被 hook 后实际是协程级非阻塞）
	int toAccept();

	~TcpAcceptor();

	NetAddress::Ptr getPeerAddr() { return peer_addr_; }

	NetAddress::Ptr getLocalAddr() { return local_addr_; }

private:
	int family_{-1};
	int fd_{-1};  // 监听 socket 的 fd

	NetAddress::Ptr local_addr_{nullptr};  // 本地监听地址
	NetAddress::Ptr peer_addr_{nullptr};   // 最近一次 accept 的客户端地址
};

class TcpServer {
public:
	using Ptr = std::shared_ptr<TcpServer>;

	TcpServer(NetAddress::Ptr addr, Dispatcher::Ptr dispatcher, Codec::Ptr codec,
			  ProtocolType type = kDefaultRpcProtocol);

	~TcpServer();

	// 启动服务器：启动 IO 线程池，在主 Reactor 中开始 accept 循环
	void start();

	void addCoroutine(Coroutine::Ptr cor);

	// 为新连接创建 TcpConnection 对象，分配给指定的 IO 线程
	TcpConnection::Ptr addClient(IOThread* io_thread, int fd);

	// 由连接 IO 线程发起，在 MainReactor 中移除并最终释放连接对象。
	void removeClient(int fd, TcpConnection::Ptr conn);

	// 刷新连接在时间轮中的位置（收到数据时调用，防止被超时淘汰）
	void freshTcpConnection(TcpTimeWheel::TcpConnectionSlot::Ptr slot);

public:
	Dispatcher::Ptr getDispatcher();

	Codec::Ptr getCodec();

	NetAddress::Ptr getPeerAddr();

	NetAddress::Ptr getLocalAddr();

	IOThreadPool::Ptr getIOThreadPool();

	TcpTimeWheel::Ptr getTimeWheel();

private:
	// accept 主协程函数：循环调用 accept，为每个新连接创建 TcpConnection
	void mainAcceptCorFunc();

private:
	NetAddress::Ptr addr_;	// 服务器监听地址

	TcpAcceptor::Ptr acceptor_;	 // 连接接收器

	int tcp_counts_{0};	 // 当前连接数

	Reactor* main_reactor_{nullptr};  // 主 Reactor（运行在主线程）

	bool is_stop_accept_{false};

	Coroutine::Ptr accept_cor_;	 // accept 协程

	Dispatcher::Ptr dispatcher_;  // 请求分发器

	Codec::Ptr codec_;	// 编解码器

	IOThreadPool::Ptr io_pool_;	 // IO 线程池

	ProtocolType protocol_type_{kDefaultRpcProtocol};  // 协议类型

	TcpTimeWheel::Ptr time_wheel_;	// 连接超时时间轮

	std::map<int, std::shared_ptr<TcpConnection>> clients_;	 // fd -> 连接映射
};

}  // namespace crpc
#endif

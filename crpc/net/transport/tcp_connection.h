#ifndef CRPC_NET_TRANSPORT_TCP_CONNECTION_H_
#define CRPC_NET_TRANSPORT_TCP_CONNECTION_H_

#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/protocol/codec.h"
#include "crpc/net/protocol/protocol_message.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_buffer.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/timeout_slot.h"

#include <map>
#include <memory>
#include <queue>
#include <string>
#include <vector>

namespace crpc {

class TcpServer;
class TcpClient;
class IOThread;

enum TcpConnectionState {
	kNotConnected = 1,	// 尚未建立连接，不能进行业务读写
	kConnected = 2,		// 连接已建立，可以进行 IO
	kHalfClosing = 3,	// 服务端调用 shutdown，写半关闭；可读但不可写
	kClosed = 4,		// 连接已关闭，不能进行 IO
};

/**
 * TCP 连接对象
 *
 * 服务端模式下，每个 TcpConnection 绑定一个协程，循环执行 input -> execute ->
 * output。 客户端模式下，TcpClient 复用该对象的缓冲区和编解码器完成一次同步 RPC
 * 调用
 */
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
	using Ptr = std::shared_ptr<TcpConnection>;

	// 服务端连接：接收请求并生成响应
	TcpConnection(TcpServer* tcp_svr, IOThread* io_thread, int fd, int buff_size,
				  NetAddress::Ptr peer_addr);

	// 客户端连接：发送请求并等待响应
	TcpConnection(TcpClient* tcp_cli, Reactor* reactor, int fd, int buff_size,
				  NetAddress::Ptr peer_addr);

	void setUpClient();

	void setUpServer();

	~TcpConnection();

	void initBuffer(int size);

	enum ConnectionType {
		kServerConnection = 1,	// 由 tcp_server 持有
		kClientConnection = 2,	// 由 tcp_client 持有
	};

public:
	// 主动关闭连接，发送 FIN 并等待后续清理
	void shutdownConnection();

	TcpConnectionState getState();

	void setState(const TcpConnectionState& state);

	TcpBuffer* getInBuffer();

	TcpBuffer* getOutBuffer();

	Codec::Ptr getCodec() const;

	// 客户端连接用 msg_req 从已解析响应表中取出对应响应包
	bool getResPackageData(const std::string& msg_req, ProtocolMessage::Ptr& data);

	// 将服务端连接注册到空闲连接时间轮
	void registerToTimeWheel();

	Coroutine::Ptr getCoroutine();

public:
	// 服务端连接协程主循环
	void mainServerLoopCorFunc();

	// 从 socket 读取数据到 read_buffer_
	void input();

	// 从 read_buffer_ 解码并分发请求，或在客户端侧缓存响应
	void execute();

	// 将 write_buffer_ 中的数据写回 socket
	void output();

	void setOverTimeFlag(bool value);

	bool getOverTimerFlag();

	void initServer();

private:
	void clearClient();

private:
	TcpServer* tcp_svr_{nullptr};
	TcpClient* tcp_cli_{nullptr};
	IOThread* io_thread_{nullptr};
	Reactor* reactor_{nullptr};

	int fd_{-1};
	TcpConnectionState state_{TcpConnectionState::kConnected};
	ConnectionType connection_type_{kServerConnection};

	NetAddress::Ptr peer_addr_;

	TcpBuffer::Ptr read_buffer_;   // socket -> 应用层的读缓冲区
	TcpBuffer::Ptr write_buffer_;  // 应用层 -> socket 的写缓冲区

	Coroutine::Ptr loop_cor_;  // 服务端连接处理协程

	Codec::Ptr codec_;	// 当前连接使用的协议编解码器

	FdEvent::Ptr fd_event_;	 // fd 在 Reactor 中的事件对象

	bool stop_{false};

	bool is_over_time_{false};	// 调用或连接是否已超时

	std::map<std::string, ProtocolMessage::Ptr> reply_datas_;  // 客户端侧 msg_req -> 响应包

	std::weak_ptr<TimeoutSlot<TcpConnection>> weak_slot_;  // 时间轮槽位，弱引用避免循环持有

	RWMutex mutex_;
};

}  // namespace crpc
#endif

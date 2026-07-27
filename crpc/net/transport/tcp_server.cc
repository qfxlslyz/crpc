#include "crpc/base/config.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/coroutine/coroutine_pool.h"
#include "crpc/net/transport/io_thread.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/tcp_server.h"

#include <utility>
#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>

namespace crpc {

extern Config::Ptr rpc_config;

TcpAcceptor::TcpAcceptor(NetAddress::Ptr net_addr) : local_addr_(net_addr) {
	family_ = local_addr_->getFamily();
}

void TcpAcceptor::init() {
	// 创建监听 socket，并绑定到配置文件指定的地址
	fd_ = socket(local_addr_->getFamily(), SOCK_STREAM, 0);
	if (fd_ < 0) {
		ErrorLog << "Start server error. socket error, sys error=" << strerror(errno);
		Exit(0);
	}
	// assert(fd_ != -1);
	DebugLog << "create listenfd succ, listenfd=" << fd_;

	// int flag = fcntl(fd_, F_GETFL, 0);
	// int rt = fcntl(fd_, F_SETFL, flag | O_NONBLOCK);

	// if (rt != 0) {
	// ErrorLog << "fcntl set nonblock error, errno=" << errno << ", error=" <<
	// strerror(errno);
	// }

	int val = 1;
	if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
		ErrorLog << "set REUSEADDR error";
	}

	socklen_t len = local_addr_->getSockLen();
	int rt = bind(fd_, local_addr_->getSockAddr(), len);
	if (rt != 0) {
		ErrorLog << "Start server error. bind error, errno=" << errno
				 << ", error=" << strerror(errno);
		Exit(0);
	}
	// assert(rt == 0);

	DebugLog << "set REUSEADDR succ";
	rt = listen(fd_, 10);
	if (rt != 0) {
		ErrorLog << "Start server error. listen error, fd= " << fd_ << ", errno=" << errno
				 << ", error=" << strerror(errno);
		Exit(0);
	}
	// assert(rt == 0);
}

TcpAcceptor::~TcpAcceptor() {
	FdEvent::Ptr fd_event = FdEventContainer::getFdContainer()->getFdEvent(fd_);
	fd_event->unregisterFromReactor();
	if (fd_ != -1) {
		close(fd_);
	}
}

int TcpAcceptor::toAccept() {
	socklen_t len = 0;
	int rt = 0;

	if (family_ == AF_INET) {
		sockaddr_in cli_addr;
		memset(&cli_addr, 0, sizeof(cli_addr));
		len = sizeof(cli_addr);
		// 调用 hook 后的 accept；没有新连接时当前 accept 协程会 Yield
		rt = AcceptHook(fd_, reinterpret_cast<sockaddr*>(&cli_addr), &len);
		if (rt == -1) {
			DebugLog << "error, no new client coming, errno=" << errno
					 << "error=" << strerror(errno);
			return -1;
		}
		InfoLog << "New client accepted succ! port:[" << cli_addr.sin_port;
		peer_addr_ = std::make_shared<IPAddress>(cli_addr);
	} else if (family_ == AF_UNIX) {  // Unix域套接字，同一主机内部通信套接字
		sockaddr_un cli_addr;
		len = sizeof(cli_addr);
		memset(&cli_addr, 0, sizeof(cli_addr));
		// 调用 hook 后的 accept
		rt = AcceptHook(fd_, reinterpret_cast<sockaddr*>(&cli_addr), &len);
		if (rt == -1) {
			DebugLog << "error, no new client coming, errno=" << errno
					 << "error=" << strerror(errno);
			return -1;
		}
		peer_addr_ = std::make_shared<UnixDomainAddress>(cli_addr);
	} else {
		ErrorLog << "unknown type protocol!";
		close(rt);
		return -1;
	}

	InfoLog << "New client accepted succ! fd:[" << rt << ", addr:[" << peer_addr_->toString()
			<< "]";
	return rt;
}

TcpServer::TcpServer(NetAddress::Ptr addr, Dispatcher::Ptr dispatcher, Codec::Ptr codec,
					 ProtocolType type /*= kDefaultRpcProtocol*/)
	: addr_(addr), dispatcher_(dispatcher), codec_(codec), protocol_type_(type) {
	if (!dispatcher_ || !codec_) {
		ErrorLog << "TcpServer setup error, dispatcher or codec is nullptr";
		Exit(0);
	}
	io_pool_ = std::make_shared<IOThreadPool>(rpc_config->iothread_num_);

	// 主 Reactor 只负责 accept，新连接的读写交给 IO 线程中的 SubReactor
	main_reactor_ = Reactor::getReactor();
	main_reactor_->setReactorType(MainReactor);

	time_wheel_ = std::make_shared<TcpTimeWheel>(main_reactor_, rpc_config->timewheel_bucket_num_,
												 rpc_config->timewheel_interval_);

	InfoLog << "TcpServer setup on [" << addr_->toString() << "]";
}

void TcpServer::start() {
	acceptor_.reset(new TcpAcceptor(addr_));
	acceptor_->init();
	accept_cor_ = GetCoroutinePool()->getCoroutineInstance();
	accept_cor_->setCallBack(std::bind(&TcpServer::mainAcceptCorFunc, this));

	// accept 协程先在主线程启动，随后进入主 Reactor 事件循环
	InfoLog << "resume accept coroutine";
	Coroutine::Resume(accept_cor_.get());

	io_pool_->start();
	main_reactor_->loop();
}

TcpServer::~TcpServer() {
	GetCoroutinePool()->returnCoroutine(accept_cor_);
	DebugLog << "~TcpServer";
}

void TcpServer::mainAcceptCorFunc() {
	while (!is_stop_accept_) {
		int fd = acceptor_->toAccept();
		if (fd == -1) {
			ErrorLog << "accept ret -1 error, return, to yield";
			Coroutine::Yield();
			continue;
		}
		// 简单轮询选择一个 IO 线程承接新连接
		IOThread* io_thread = io_pool_->getIOThread();
		TcpConnection::Ptr conn = addClient(io_thread, fd);
		conn->initServer();
		DebugLog << "tcpconnection address is " << conn.get() << ", and fd is" << fd;

		// auto cb = [io_thread, conn]() mutable {
		//   io_thread->addClient(conn.get());
		//   conn.reset();
		// };

		// 连接协程投递到目标 IO 线程，后续由对应 SubReactor 驱动读写
		io_thread->getReactor()->addCoroutine(conn->getCoroutine());
		tcp_counts_++;
		DebugLog << "current tcp connection count is [" << tcp_counts_ << "]";
	}
}

void TcpServer::addCoroutine(Coroutine::Ptr cor) {
	main_reactor_->addCoroutine(cor);
}

TcpConnection::Ptr TcpServer::addClient(IOThread* io_thread, int fd) {
	auto it = clients_.find(fd);
	if (it != clients_.end()) {
		it->second.reset();
		// 设置新的 TcpConnection
		DebugLog << "fd " << fd << "have exist, reset it";
		it->second = std::make_shared<TcpConnection>(this, io_thread, fd, 128, getPeerAddr());
		return it->second;

	} else {
		DebugLog << "fd " << fd << "did't exist, new it";
		TcpConnection::Ptr conn =
			std::make_shared<TcpConnection>(this, io_thread, fd, 128, getPeerAddr());
		clients_.insert(std::make_pair(fd, conn));
		return conn;
	}
}

void TcpServer::removeClient(int fd, TcpConnection::Ptr conn) {
	main_reactor_->addTask(
		[this, fd, conn = std::move(conn)]() {
			auto it = clients_.find(fd);
			if (it != clients_.end() && it->second.get() == conn.get()) {
				DebugLog << "remove closed TcpConnection [fd:" << fd << "] in MainReactor";
				clients_.erase(it);
			}

			// fd 已复用时 map 中会是新连接，不能删除它；但旧连接仍应从计数中扣除。
			if (tcp_counts_ > 0) {
				--tcp_counts_;
			}
		},
		true);
}

void TcpServer::freshTcpConnection(TcpTimeWheel::TcpConnectionSlot::Ptr slot) {
	// 数据读取发生在连接所属的 SubReactor；时间轮归 MainReactor 管理。
	// 通过任务投递串行修改桶结构，避免多个 IO 线程并发访问 wheel_。
	auto cb = [slot, this]() mutable {
		this->getTimeWheel()->fresh(slot);
		slot.reset();
	};
	main_reactor_->addTask(cb);
}

NetAddress::Ptr TcpServer::getPeerAddr() {
	return acceptor_->getPeerAddr();
}

NetAddress::Ptr TcpServer::getLocalAddr() {
	return addr_;
}

TcpTimeWheel::Ptr TcpServer::getTimeWheel() {
	return time_wheel_;
}

IOThreadPool::Ptr TcpServer::getIOThreadPool() {
	return io_pool_;
}

Dispatcher::Ptr TcpServer::getDispatcher() {
	return dispatcher_;
}

Codec::Ptr TcpServer::getCodec() {
	return codec_;
}

}  // namespace crpc

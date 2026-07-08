#include "crpc/base/error_code.h"
#include "crpc/base/log.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/coroutine/coroutine_pool.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/timer.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_client.h"

#include <arpa/inet.h>
#include <sys/socket.h>

namespace crpc {

TcpClient::TcpClient(NetAddress::Ptr addr, Codec::Ptr codec,
					 ProtocolType type /*= kDefaultRpcProtocol*/)
	: peer_addr_(addr), codec_(codec) {
	(void)type;

	if (!codec_) {
		ErrorLog << "TcpClient setup error, codec is nullptr";
		return;
	}

	family_ = peer_addr_->getFamily();
	// socket 在 ConnectHook/ReadHook/WriteHook
	// 中会被设置为协程友好的非阻塞模式
	fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (fd_ == -1) {
		ErrorLog << "call socket error, fd=-1, sys error=" << strerror(errno);
	}
	DebugLog << "TcpClient() create fd = " << fd_;
	local_addr_ = std::make_shared<IPAddress>("127.0.0.1", 0);
	reactor_ = Reactor::getReactor();

	connection_ = std::make_shared<TcpConnection>(this, reactor_, fd_, 128, peer_addr_);
}

TcpClient::~TcpClient() {
	if (fd_ > 0) {
		FdEventContainer::getFdContainer()->getFdEvent(fd_)->unregisterFromReactor();
		close(fd_);
		DebugLog << "~TcpClient() close fd = " << fd_;
	}
}

TcpConnection* TcpClient::getConnection() {
	if (!connection_.get()) {
		connection_ = std::make_shared<TcpConnection>(this, reactor_, fd_, 128, peer_addr_);
	}
	return connection_.get();
}
void TcpClient::resetFd() {
	// connect 失败后旧 fd 状态不可复用，重新创建 fd 并继续下一次尝试
	FdEvent::Ptr fd_event = FdEventContainer::getFdContainer()->getFdEvent(fd_);
	fd_event->unregisterFromReactor();
	close(fd_);
	fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (fd_ == -1) {
		ErrorLog << "call socket error, fd=-1, sys error=" << strerror(errno);
	} else {
	}
}

int TcpClient::sendAndRecv(const std::string& msg_no, ProtocolMessage::Ptr& res) {
	bool is_timeout = false;
	Coroutine* cur_cor = Coroutine::getCurrentCoroutine();
	// 超时事件触发时恢复当前协程，使 sendAndRecv 能从阻塞式等待中返回错误
	auto timer_cb = [this, &is_timeout, cur_cor]() {
		InfoLog << "TcpClient timer out event occur";
		is_timeout = true;
		this->connection_->setOverTimeFlag(true);
		Coroutine::Resume(cur_cor);
	};
	TimerEvent::Ptr event = std::make_shared<TimerEvent>(max_timeout_, false, timer_cb);
	reactor_->getTimer()->addTimerEvent(event);

	DebugLog << "add rpc timer event, timeout on " << event->arrive_time_;

	// 先建立连接；ConnectHook 内部会在 EINPROGRESS 时让出协程等待可写事件
	while (!is_timeout) {
		DebugLog << "begin to connect";
		if (connection_->getState() != kConnected) {
			int rt = ConnectHook(fd_, reinterpret_cast<sockaddr*>(peer_addr_->getSockAddr()),
								 peer_addr_->getSockLen());
			if (rt == 0) {
				DebugLog << "connect [" << peer_addr_->toString() << "] succ!";
				connection_->setUpClient();
				break;
			}
			resetFd();
			if (is_timeout) {
				InfoLog << "connect timeout, break";
				goto err_deal;
			}
			if (errno == ECONNREFUSED) {
				std::stringstream ss;
				ss << "connect error, peer[ " << peer_addr_->toString() << " ] closed.";
				err_info_ = ss.str();
				ErrorLog << "cancle overtime event, err info=" << err_info_;
				reactor_->getTimer()->delTimerEvent(event);
				return ERROR_PEER_CLOSED;
			}
			if (errno == EAFNOSUPPORT) {
				std::stringstream ss;
				ss << "connect cur sys ror, errinfo is " << std::string(strerror(errno))
				   << " ] closed.";
				err_info_ = ss.str();
				ErrorLog << "cancle overtime event, err info=" << err_info_;
				reactor_->getTimer()->delTimerEvent(event);
				return ERROR_CONNECT_SYS_ERR;
			}
		} else {
			break;
		}
	}

	if (connection_->getState() != kConnected) {
		std::stringstream ss;
		ss << "connect peer addr[" << peer_addr_->toString()
		   << "] error. sys error=" << strerror(errno);
		err_info_ = ss.str();
		reactor_->getTimer()->delTimerEvent(event);
		return ERROR_FAILED_CONNECT;
	}

	connection_->setUpClient();
	// 请求包已经由 CrpcChannel 编码到连接写缓冲区，这里负责把它发送出去
	connection_->output();
	if (connection_->getOverTimerFlag()) {
		InfoLog << "send data over time";
		is_timeout = true;
		goto err_deal;
	}

	// 读取并解码响应，直到拿到与本次请求 msg_req 匹配的包或出现错误
	while (!connection_->getResPackageData(msg_no, res)) {
		DebugLog << "redo getResPackageData";
		connection_->input();

		if (connection_->getOverTimerFlag()) {
			InfoLog << "read data over time";
			is_timeout = true;
			goto err_deal;
		}
		if (connection_->getState() == kClosed) {
			InfoLog << "peer close";
			goto err_deal;
		}

		connection_->execute();
	}

	reactor_->getTimer()->delTimerEvent(event);
	err_info_ = "";
	return 0;

err_deal:
	// 连接出错时需要关闭 fd 并重新打开一个新的 fd
	FdEventContainer::getFdContainer()->getFdEvent(fd_)->unregisterFromReactor();
	close(fd_);
	fd_ = socket(AF_INET, SOCK_STREAM, 0);
	std::stringstream ss;
	if (is_timeout) {
		ss << "call rpc falied, over " << max_timeout_ << " ms";
		err_info_ = ss.str();

		connection_->setOverTimeFlag(false);
		return ERROR_RPC_CALL_TIMEOUT;
	} else {
		ss << "call rpc falied, peer closed [" << peer_addr_->toString() << "]";
		err_info_ = ss.str();
		return ERROR_PEER_CLOSED;
	}
}

void TcpClient::stop() {
	if (!is_stop_) {
		is_stop_ = true;
		reactor_->stop();
	}
}

}  // namespace crpc

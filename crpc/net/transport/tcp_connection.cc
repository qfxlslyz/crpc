#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/coroutine/coroutine_pool.h"
#include "crpc/net/event/timer.h"
#include "crpc/net/transport/tcp_client.h"
#include "crpc/net/transport/tcp_connection.h"
#include "crpc/net/transport/tcp_connection_time_wheel.h"
#include "crpc/net/transport/tcp_server.h"
#include "crpc/net/transport/timeout_slot.h"

#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

namespace crpc {

TcpConnection::TcpConnection(TcpServer* tcp_svr, IOThread* io_thread, int fd, int buff_size,
							 NetAddress::Ptr peer_addr)
	: io_thread_(io_thread),
	  fd_(fd),
	  state_(kConnected),
	  connection_type_(kServerConnection),
	  peer_addr_(peer_addr) {
	reactor_ = io_thread_->getReactor();

	// 服务端连接复用服务器统一的编解码器和分发器，后续在 IO 线程协程中处理
	tcp_svr_ = tcp_svr;

	codec_ = tcp_svr_->getCodec();
	fd_event_ = FdEventContainer::getFdContainer()->getFdEvent(fd);
	fd_event_->setReactor(reactor_);
	initBuffer(buff_size);
	server_conn_cor_ = GetCoroutinePool()->getCoroutineInstance();
	state_ = kConnected;
	DebugLog << "succ create tcp connection[" << state_ << "], fd=" << fd;
}

TcpConnection::TcpConnection(TcpClient* tcp_cli, Reactor* reactor, int fd, int buff_size,
							 NetAddress::Ptr peer_addr)
	: fd_(fd), state_(kNotConnected), connection_type_(kClientConnection), peer_addr_(peer_addr) {
	reactor_ = reactor;

	// 客户端连接不注册独立循环协程，由 TcpClient::sendAndRecv 主动驱动读写
	tcp_cli_ = tcp_cli;

	codec_ = tcp_cli_->getCodec();

	fd_event_ = FdEventContainer::getFdContainer()->getFdEvent(fd);
	fd_event_->setReactor(reactor_);
	initBuffer(buff_size);

	DebugLog << "succ create tcp connection[kNotConnected]";
}

void TcpConnection::initServer() {
	// 新连接建立后先放入时间轮，再初始化连接处理协程
	registerToTimeWheel();
	server_conn_cor_->setCallBack(std::bind(&TcpConnection::runServerConnectionLoop, this));
}

void TcpConnection::setUpServer() {
	// 将连接协程投递到所属 IO 线程的 Reactor 中执行
	reactor_->addCoroutine(server_conn_cor_);
}

void TcpConnection::registerToTimeWheel() {
	// 槽位过期时关闭连接；收到数据时 TcpServer 会刷新槽位延长生存时间
	auto cb = [](TcpConnection::Ptr conn) { conn->shutdownConnection(); };
	TcpTimeWheel::TcpConnectionSlot::Ptr tmp =
		std::make_shared<TimeoutSlot<TcpConnection>>(shared_from_this(), cb);
	weak_slot_ = tmp;
	tcp_svr_->freshTcpConnection(tmp);
}

void TcpConnection::setUpClient() {
	setState(kConnected);
}

TcpConnection::~TcpConnection() {
	if (connection_type_ == kServerConnection) {
		GetCoroutinePool()->returnCoroutine(server_conn_cor_);
	}

	DebugLog << "~TcpConnection, fd=" << fd_;
}

void TcpConnection::initBuffer(int size) {
	// 初始化缓冲区大小
	write_buffer_ = std::make_shared<TcpBuffer>(size);
	read_buffer_ = std::make_shared<TcpBuffer>(size);
}

void TcpConnection::runServerConnectionLoop() {
	while (!stop_) {
		// 每次被 IO 事件唤醒后，尽量完成读取、业务处理和响应发送
		input();

		execute();

		output();
	}
	InfoLog << "this connection has already end loop";
}

void TcpConnection::input() {
	if (is_over_time_) {
		InfoLog << "over timer, skip input progress";
		return;
	}
	TcpConnectionState state = getState();
	if (state == kClosed || state == kNotConnected) {
		return;
	}
	bool read_all = false;
	bool close_flag = false;
	int count = 0;
	while (!read_all) {
		// 即使在 LT 模式下，尽量一次读完数据也能减少后续 epoll_wait 的重复唤醒
		if (read_buffer_->writeAble() == 0) {
			read_buffer_->resizeBuffer(2 * read_buffer_->getSize());
		}

		int read_count = read_buffer_->writeAble();
		int write_index = read_buffer_->writeIndex();

		DebugLog << "read_buffer_ size=" << read_buffer_->getBufferVector().size()
				 << "rd=" << read_buffer_->readIndex() << "wd=" << read_buffer_->writeIndex();
		int rt = ReadHook(fd_, &(read_buffer_->buffer_[write_index]), read_count);
		if (rt > 0) {
			read_buffer_->recycleWrite(rt);
		}
		DebugLog << "read_buffer_ size=" << read_buffer_->getBufferVector().size()
				 << "rd=" << read_buffer_->readIndex() << "wd=" << read_buffer_->writeIndex();

		DebugLog << "read data back, fd=" << fd_;
		count += rt;
		if (is_over_time_) {
			InfoLog << "over timer, now break read function";
			break;
		}
		if (rt <= 0) {
			DebugLog << "rt <= 0";
			ErrorLog << "read empty while occur read event, because of peer "
						"close, fd= "
					 << fd_ << ", sys error=" << strerror(errno) << ", now to clear tcp connection";
			// 当前协程可以销毁
			close_flag = true;
			break;
		} else {
			if (rt == read_count) {
				DebugLog << "read_count == rt";
				// 可能还有更多数据，需要继续读取
				continue;
			} else if (rt < read_count) {
				DebugLog << "read_count > rt";
				// 已读取完 socket 缓冲区中的所有数据，跳出循环
				read_all = true;
				break;
			}
		}
	}
	if (close_flag) {
		clearClient();
		// fd 已关闭，当前协程不能再次被 Reactor 恢复，等待主线程清理连接对象
		DebugLog << "peer close, now yield current coroutine, wait main thread "
					"clear this TcpConnection";
		Coroutine::getCurrentCoroutine()->setCanResume(false);
		Coroutine::Yield();
	}

	if (is_over_time_) {
		return;
	}

	if (!read_all) {
		ErrorLog << "not read all data in socket buffer";
	}
	InfoLog << "recv [" << count << "] bytes data from [" << peer_addr_->toString() << "], fd ["
			<< fd_ << "]";
	if (connection_type_ == kServerConnection) {
		// 服务端连接有数据交互时刷新时间轮，避免活跃连接被误判为空闲
		TcpTimeWheel::TcpConnectionSlot::Ptr tmp = weak_slot_.lock();
		if (tmp) {
			tcp_svr_->freshTcpConnection(tmp);
		}
	}
}

void TcpConnection::execute() {
	// 读缓冲区可能包含多个完整包，循环解码直到数据不足或解析失败
	while (read_buffer_->readAble() > 0) {
		ProtocolMessage::Ptr data = codec_->createData();

		codec_->decode(read_buffer_.get(), data.get());
		if (!data->decode_succ) {
			// 半包时 decode_succ 为 false，保留剩余数据等待下一次读事件
			ErrorLog << "it parse request error of fd " << fd_;
			break;
		}
		if (connection_type_ == kServerConnection) {
			// 服务端收到请求后立即分发并把响应写入 write_buffer_
			tcp_svr_->getDispatcher()->dispatch(data.get(), codec_.get(), write_buffer_.get());
		} else if (connection_type_ == kClientConnection) {
			// 客户端收到响应后先按 msg_req 缓存，sendAndRecv 再取出目标响应
			std::string msg_req = codec_->getMsgReq(data.get());
			if (!msg_req.empty()) {
				reply_datas_.insert(std::make_pair(msg_req, data));
			}
		}
	}
}

void TcpConnection::output() {
	if (is_over_time_) {
		InfoLog << "over timer, skip output progress";
		return;
	}
	while (true) {
		TcpConnectionState state = getState();
		if (state != kConnected) {
			break;
		}

		if (write_buffer_->readAble() == 0) {
			// 没有待发送数据时让出执行权，等待下一次读事件或业务写入
			DebugLog << "app buffer of fd[" << fd_ << "] no data to write, to yiled this coroutine";
			break;
		}

		int total_size = write_buffer_->readAble();
		int read_index = write_buffer_->readIndex();
		int rt = WriteHook(fd_, &(write_buffer_->buffer_[read_index]), total_size);
		// 日志：写入结束
		if (rt <= 0) {
			ErrorLog << "write empty, error=" << strerror(errno);
		}

		DebugLog << "succ write " << rt << " bytes";
		write_buffer_->recycleRead(rt);
		DebugLog << "recycle write index =" << write_buffer_->writeIndex()
				 << ", read_index =" << write_buffer_->readIndex()
				 << "readable = " << write_buffer_->readAble();
		InfoLog << "send[" << rt << "] bytes data to [" << peer_addr_->toString() << "], fd ["
				<< fd_ << "]";
		if (write_buffer_->readAble() <= 0) {
			// 所有数据已发送，本轮连接处理结束
			InfoLog << "send all data, now unregister write event and break";
			// fd_event_->delListenEvents(IOEvent::kWrite);
			break;
		}

		if (is_over_time_) {
			InfoLog << "over timer, now break write function";
			break;
		}
	}
}

void TcpConnection::clearClient() {
	if (getState() == kClosed) {
		DebugLog << "this client has closed";
		return;
	}
	// 先注销 epoll 事件，避免关闭后的 fd 继续触发旧事件
	fd_event_->unregisterFromReactor();

	// 停止读写协程
	stop_ = true;

	close(fd_event_->getFd());
	setState(kClosed);
}

void TcpConnection::shutdownConnection() {
	TcpConnectionState state = getState();
	if (state == kClosed || state == kNotConnected) {
		DebugLog << "this client has closed";
		return;
	}
	setState(kHalfClosing);
	InfoLog << "shutdown conn[" << peer_addr_->toString() << "], fd=" << fd_;
	// 调用系统 shutdown 发送 FIN
	// 等待客户端处理完后发送 FIN
	// 随后 fd 会产生可读事件，但读取到的字节数为 0
	// 然后调用 clearClient 将状态设置为 CLOSED
	// IOThread::MainLoopTimerFunc 会删除 CLOSED 状态的连接
	shutdown(fd_event_->getFd(), SHUT_RDWR);
}

TcpBuffer* TcpConnection::getInBuffer() {
	return read_buffer_.get();
}

TcpBuffer* TcpConnection::getOutBuffer() {
	return write_buffer_.get();
}

bool TcpConnection::getResPackageData(const std::string& msg_req, ProtocolMessage::Ptr& data) {
	auto it = reply_datas_.find(msg_req);
	if (it != reply_datas_.end()) {
		DebugLog << "return a resdata";
		data = it->second;
		reply_datas_.erase(it);
		return true;
	}
	DebugLog << msg_req << "|reply data not exist";
	return false;
}

Codec::Ptr TcpConnection::getCodec() const {
	return codec_;
}

TcpConnectionState TcpConnection::getState() {
	TcpConnectionState state;
	RWMutex::ReadScopedLock lock(mutex_);
	state = state_;
	lock.unlock();

	return state;
}

void TcpConnection::setState(const TcpConnectionState& state) {
	RWMutex::WriteScopedLock lock(mutex_);
	state_ = state;
	lock.unlock();
}

void TcpConnection::setOverTimeFlag(bool value) {
	is_over_time_ = value;
}

bool TcpConnection::getOverTimerFlag() {
	return is_over_time_;
}

Coroutine::Ptr TcpConnection::getCoroutine() {
	return server_conn_cor_;
}

}  // namespace crpc

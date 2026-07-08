#include "crpc/base/config.h"
#include "crpc/base/log.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/event/fd_event.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/event/timer.h"

#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <dlfcn.h>

AcceptFunPtr g_sys_accept_fun = reinterpret_cast<AcceptFunPtr>(dlsym(RTLD_NEXT, "accept"));
ReadFunPtr g_sys_read_fun = reinterpret_cast<ReadFunPtr>(dlsym(RTLD_NEXT, "read"));
WriteFunPtr g_sys_write_fun = reinterpret_cast<WriteFunPtr>(dlsym(RTLD_NEXT, "write"));
ConnectFunPtr g_sys_connect_fun = reinterpret_cast<ConnectFunPtr>(dlsym(RTLD_NEXT, "connect"));
SleepFunPtr g_sys_sleep_fun = reinterpret_cast<SleepFunPtr>(dlsym(RTLD_NEXT, "sleep"));

// static int g_hook_enable = false;

// static int g_max_timeout = 75000;

namespace crpc {

extern Config::Ptr rpc_config;

static bool g_hook = true;

void SetHook(bool value) {
	g_hook = value;
}

void ToEpoll(FdEvent::Ptr fd_event, int events) {
	Coroutine* cur_cor = Coroutine::getCurrentCoroutine();
	if (events & IOEvent::kRead) {
		DebugLog << "fd:[" << fd_event->getFd() << "], register read event to epoll";
		// fd_event->setCallBack(IOEvent::kRead,
		//   [cur_cor, fd_event]() {
		//     Coroutine::Resume(cur_cor);
		//   }
		// );
		fd_event->setCoroutine(cur_cor);
		fd_event->addListenEvents(IOEvent::kRead);
	}
	if (events & IOEvent::kWrite) {
		DebugLog << "fd:[" << fd_event->getFd() << "], register write event to epoll";
		// fd_event->setCallBack(IOEvent::kWrite,
		//   [cur_cor]() {
		//     Coroutine::Resume(cur_cor);
		//   }
		// );
		fd_event->setCoroutine(cur_cor);
		fd_event->addListenEvents(IOEvent::kWrite);
	}
	// fd_event->updateToReactor();
}

ssize_t ReadHook(int fd, void* buf, size_t count) {
	DebugLog << "this is hook read";
	if (Coroutine::isMainCoroutine()) {
		DebugLog << "hook disable, call sys read func";
		return g_sys_read_fun(fd, buf, count);
	}

	Reactor::getReactor();
	// assert(reactor != nullptr);

	FdEvent::Ptr fd_event = FdEventContainer::getFdContainer()->getFdEvent(fd);
	if (fd_event->getReactor() == nullptr) {
		fd_event->setReactor(Reactor::getReactor());
	}

	// if (fd_event->isNonBlock()) {
	// DebugLog << "user set nonblock, call sys func";
	// return g_sys_read_fun(fd, buf, count);
	// }

	fd_event->setNonBlock();

	// 必须先把读事件注册到 epoll
	// 因为连接 sockfd 创建后，Reactor 应持续关注读事件
	// 如果先调用系统 read 且读取成功，本函数会直接返回而不会注册读事件
	// 这样该连接 sockfd 后续将不会被 Reactor 关注读事件
	ssize_t n = g_sys_read_fun(fd, buf, count);
	if (n > 0) {
		return n;
	}

	ToEpoll(fd_event, IOEvent::kRead);

	DebugLog << "read func to yield";
	//让出协程的执行权，切换回主协程，等待 IO 就绪后被Resume 恢复执行
	Coroutine::Yield();

	fd_event->delListenEvents(IOEvent::kRead);
	fd_event->clearCoroutine();
	// fd_event->updateToReactor();

	DebugLog << "read func yield back, now to call sys read";
	return g_sys_read_fun(fd, buf, count);
}

int AcceptHook(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
	DebugLog << "this is hook accept";
	if (Coroutine::isMainCoroutine()) {
		DebugLog << "hook disable, call sys accept func";
		return g_sys_accept_fun(sockfd, addr, addrlen);
	}
	Reactor::getReactor();
	// assert(reactor != nullptr);

	FdEvent::Ptr fd_event = FdEventContainer::getFdContainer()->getFdEvent(sockfd);
	if (fd_event->getReactor() == nullptr) {
		fd_event->setReactor(Reactor::getReactor());
	}

	// if (fd_event->isNonBlock()) {
	// DebugLog << "user set nonblock, call sys func";
	// return g_sys_accept_fun(sockfd, addr, addrlen);
	// }

	fd_event->setNonBlock();

	int n = g_sys_accept_fun(sockfd, addr, addrlen);
	if (n > 0) {
		return n;
	}

	ToEpoll(fd_event, IOEvent::kRead);

	DebugLog << "accept func to yield";
	Coroutine::Yield();

	fd_event->delListenEvents(IOEvent::kRead);
	fd_event->clearCoroutine();
	// fd_event->updateToReactor();

	DebugLog << "accept func yield back, now to call sys accept";
	return g_sys_accept_fun(sockfd, addr, addrlen);
}

ssize_t WriteHook(int fd, const void* buf, size_t count) {
	DebugLog << "this is hook write";
	if (Coroutine::isMainCoroutine()) {
		DebugLog << "hook disable, call sys write func";
		return g_sys_write_fun(fd, buf, count);
	}
	Reactor::getReactor();
	// assert(reactor != nullptr);

	FdEvent::Ptr fd_event = FdEventContainer::getFdContainer()->getFdEvent(fd);
	if (fd_event->getReactor() == nullptr) {
		fd_event->setReactor(Reactor::getReactor());
	}

	// if (fd_event->isNonBlock()) {
	// DebugLog << "user set nonblock, call sys func";
	// return g_sys_write_fun(fd, buf, count);
	// }

	fd_event->setNonBlock();

	ssize_t n = g_sys_write_fun(fd, buf, count);
	if (n > 0) {
		return n;
	}

	ToEpoll(fd_event, IOEvent::kWrite);

	DebugLog << "write func to yield";
	Coroutine::Yield();

	fd_event->delListenEvents(IOEvent::kWrite);
	fd_event->clearCoroutine();
	// fd_event->updateToReactor();

	DebugLog << "write func yield back, now to call sys write";
	return g_sys_write_fun(fd, buf, count);
}

int ConnectHook(int sockfd, const struct sockaddr* addr, socklen_t addrlen) {
	DebugLog << "this is hook connect";
	if (Coroutine::isMainCoroutine()) {
		DebugLog << "hook disable, call sys connect func";
		return g_sys_connect_fun(sockfd, addr, addrlen);
	}
	Reactor* reactor = Reactor::getReactor();
	// assert(reactor != nullptr);

	FdEvent::Ptr fd_event = FdEventContainer::getFdContainer()->getFdEvent(sockfd);
	if (fd_event->getReactor() == nullptr) {
		fd_event->setReactor(reactor);
	}
	Coroutine* cur_cor = Coroutine::getCurrentCoroutine();

	// if (fd_event->isNonBlock()) {
	// DebugLog << "user set nonblock, call sys func";
	// return g_sys_connect_fun(sockfd, addr, addrlen);
	// }

	fd_event->setNonBlock();
	int n = g_sys_connect_fun(sockfd, addr, addrlen);
	if (n == 0) {
		DebugLog << "direct connect succ, return";
		return n;
	} else if (errno != EINPROGRESS) {
		DebugLog << "connect error and errno is't EINPROGRESS, errno=" << errno
				 << ",error=" << strerror(errno);
		return n;
	}

	DebugLog << "errno == EINPROGRESS";

	ToEpoll(fd_event, IOEvent::kWrite);

	bool is_timeout = false;  // 是否超时

	// 超时函数句柄
	auto timeout_cb = [&is_timeout, cur_cor]() {
		// 设置超时标志，然后唤醒协程
		is_timeout = true;
		Coroutine::Resume(cur_cor);	 //这里唤醒
	};

	TimerEvent::Ptr event =
		std::make_shared<TimerEvent>(rpc_config->max_connect_timeout_, false, timeout_cb);

	Timer* timer = reactor->getTimer();
	timer->addTimerEvent(event);

	Coroutine::Yield();	 //这里让出cpu权，等待连接成功或者超时后被 Resume
						 //恢复执行

	// write事件需要删除，因为连接成功后后面会重新监听该fd的写事件
	fd_event->delListenEvents(IOEvent::kWrite);
	fd_event->clearCoroutine();
	// fd_event->updateToReactor();

	// 定时器也需要删除
	timer->delTimerEvent(event);

	n = g_sys_connect_fun(sockfd, addr, addrlen);
	if ((n < 0 && errno == EISCONN) || n == 0) {
		DebugLog << "connect succ";
		return 0;
	}

	if (is_timeout) {
		ErrorLog << "connect error,  timeout[ " << rpc_config->max_connect_timeout_ << "ms]";
		errno = ETIMEDOUT;
	}

	DebugLog << "connect error and errno=" << errno << ", error=" << strerror(errno);
	return -1;
}

unsigned int SleepHook(unsigned int seconds) {
	DebugLog << "this is hook sleep";
	if (Coroutine::isMainCoroutine()) {
		DebugLog << "hook disable, call sys sleep func";
		return g_sys_sleep_fun(seconds);
	}

	Coroutine* cur_cor = Coroutine::getCurrentCoroutine();

	bool is_timeout = false;
	auto timeout_cb = [cur_cor, &is_timeout]() {
		DebugLog << "onTime, now resume sleep cor";
		is_timeout = true;
		// 设置超时标志，然后唤醒协程
		Coroutine::Resume(cur_cor);
	};

	TimerEvent::Ptr event = std::make_shared<TimerEvent>(1000 * seconds, false, timeout_cb);

	Reactor::getReactor()->getTimer()->addTimerEvent(event);

	DebugLog << "now to yield sleep";
	// read 或 write
	// 也可能恢复该协程，因此协程恢复后必须检查是否超时，否则需要再次 Yield
	while (!is_timeout) {
		Coroutine::Yield();
	}

	// 定时器也需要删除
	// Reactor::getReactor()->getTimer()->delTimerEvent(event);

	return 0;
}

}  // namespace crpc

extern "C" {

int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
	if (!crpc::g_hook) {
		return g_sys_accept_fun(sockfd, addr, addrlen);
	} else {
		return crpc::AcceptHook(sockfd, addr, addrlen);
	}
}

ssize_t read(int fd, void* buf, size_t count) {
	if (!crpc::g_hook) {
		return g_sys_read_fun(fd, buf, count);
	} else {
		return crpc::ReadHook(fd, buf, count);
	}
}

ssize_t write(int fd, const void* buf, size_t count) {
	if (!crpc::g_hook) {
		return g_sys_write_fun(fd, buf, count);
	} else {
		return crpc::WriteHook(fd, buf, count);
	}
}

int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen) {
	if (!crpc::g_hook) {
		return g_sys_connect_fun(sockfd, addr, addrlen);
	} else {
		return crpc::ConnectHook(sockfd, addr, addrlen);
	}
}

unsigned int sleep(unsigned int seconds) {
	if (!crpc::g_hook) {
		return g_sys_sleep_fun(seconds);
	} else {
		return crpc::SleepHook(seconds);
	}
}
}

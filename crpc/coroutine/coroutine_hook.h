/**
 * 系统调用 Hook 模块
 *
 * 核心思想：通过覆盖 glibc 的 read/write/connect/accept/sleep 等系统调用，
 * 将阻塞式 IO 转变为协程级别的非阻塞 IO
 *
 * 当 hook 后的函数遇到 EAGAIN（数据未就绪）时：
 *   1. 将 fd 注册到 epoll 监听
 *   2. Yield 让出当前协程
 *   3. epoll 通知 IO 就绪后，Resume 恢复协程继续执行
 *
 * 这样上层业务代码可以用同步写法实现异步效果
 */
#ifndef CRPC_COROUTINE_COROUTINE_HOOK_H_
#define CRPC_COROUTINE_COROUTINE_HOOK_H_

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

// 原始系统调用的函数指针类型，通过 dlsym 获取原始实现
using ReadFunPtr = ssize_t (*)(int fd, void* buf, size_t count);

using WriteFunPtr = ssize_t (*)(int fd, const void* buf, size_t count);

using ConnectFunPtr = int (*)(int sockfd, const struct sockaddr* addr, socklen_t addrlen);

using AcceptFunPtr = int (*)(int sockfd, struct sockaddr* addr, socklen_t* addrlen);

using SocketFunPtr = int (*)(int domain, int type, int protocol);

using SleepFunPtr = int (*)(unsigned int seconds);

namespace crpc {

// Hook 版本的系统调用实现（内部实现协程切换逻辑）
int AcceptHook(int sockfd, struct sockaddr* addr, socklen_t* addrlen);

ssize_t ReadHook(int fd, void* buf, size_t count);

ssize_t WriteHook(int fd, const void* buf, size_t count);

int ConnectHook(int sockfd, const struct sockaddr* addr, socklen_t addrlen);

unsigned int SleepHook(unsigned int seconds);

// 开启/关闭当前线程的 hook 功能
void SetHook(bool);

}  // namespace crpc
// 覆盖 C 标准库的同名函数，链接时优先使用这些实现
extern "C" {

int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen);

ssize_t read(int fd, void* buf, size_t count);

ssize_t write(int fd, const void* buf, size_t count);

int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen);

unsigned int sleep(unsigned int seconds);
}

#endif

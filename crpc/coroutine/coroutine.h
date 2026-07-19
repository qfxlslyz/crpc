/**
 * 协程模块
 * 实现非对称协程（asymmetric
 * coroutine），每个协程拥有独立的栈空间和寄存器上下文。 协程通过 Yield/Resume
 * 在主协程和子协程之间切换，配合 epoll 实现同步编程风格的异步 IO
 *
 * 核心原理:
 *   - 每个线程有一个主协程（调度协程），负责 epoll_wait 和事件分发
 *   - IO 操作被 hook 后，遇到 EAGAIN 时 Yield 让出 CPU，等 IO 就绪后 Resume
 * 恢复执行
 *   - 切换通过 coctx_swap 汇编函数保存/恢复寄存器完成
 */
#ifndef CRPC_COROUTINE_COROUTINE_H_
#define CRPC_COROUTINE_COROUTINE_H_

#include "crpc/base/run_time.h"
#include "crpc/coroutine/coctx.h"

#include <atomic>
#include <functional>
#include <memory>

namespace crpc {

int GetCoroutineIndex();

RunTime* GetCurrentRunTime();

void SetCurrentRunTime(RunTime* v);

class Coroutine {
public:
	using Ptr = std::shared_ptr<Coroutine>;

private:
	// 私有默认构造，仅用于创建主协程（每个线程的调度协程）
	Coroutine();

public:
	// 创建子协程，需提供栈空间（不带回调，后续通过 setCallBack 设置）
	Coroutine(int size, char* stack_ptr);

	// 创建子协程，同时指定栈空间和执行函数
	Coroutine(int size, char* stack_ptr, std::function<void()> cb);

	~Coroutine();

	bool setCallBack(std::function<void()> cb);

	int getCorId() const { return cor_id_; }

	void setIsInCoFunc(const bool v) { is_in_cofunc_.store(v); }

	bool getIsInCoFunc() const { return is_in_cofunc_.load(); }

	std::string getMsgNo() { return msg_no_; }

	RunTime* getRunTime() { return &run_time_; }

	void setMsgNo(const std::string& msg_no) { msg_no_ = msg_no; }

	void setIndex(int index) { index_ = index; }

	int getIndex() { return index_; }

	char* getStackPtr() { return stack_sp_; }

	int getStackSize() { return stack_size_; }

	void setCanResume(bool v) { can_resume_ = v; }

public:
	// 让出当前协程的执行权，切换回主协程
	static void Yield();

	// 恢复指定协程的执行，从主协程切换到目标子协程
	static void Resume(Coroutine* cor);

	// 获取当前线程正在执行的协程
	static Coroutine* getCurrentCoroutine();

	// 获取当前线程的主协程（调度协程）
	static Coroutine* getMainCoroutine();

	static bool isMainCoroutine();

private:
	int cor_id_{0};			   // 协程唯一 ID
	coctx coctx_;			   // 寄存器上下文，用于协程切换时保存/恢复 CPU 状态
	int stack_size_{0};		   // 协程栈空间大小（字节）
	char* stack_sp_{nullptr};  // 协程栈空间起始地址（由协程池的 Memory 分配）
	std::atomic<bool> is_in_cofunc_{false};	 // 是否正在执行协程函数
	std::string msg_no_;					 // 当前处理的请求 ID
	RunTime run_time_;						 // 协程运行时上下文

	bool can_resume_{true};	 // 是否允许被 Resume

	int index_{-1};	 // 在协程池中的索引位置

public:
	std::function<void()> call_back_;  // 协程执行的业务函数
};

}  // namespace crpc

#endif

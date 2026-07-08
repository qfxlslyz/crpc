/**
 * 协程池
 * 预先创建一批协程对象并复用，避免频繁创建/销毁协程带来的开销
 * 每个协程的栈内存由 Memory 对象统一管理
 *
 * 使用流程:
 *   1. getCoroutineInstance() 从池中获取一个空闲协程
 *   2. 为协程设置回调函数后 Resume 执行
 *   3. 执行完毕后 returnCoroutine() 归还到池中
 */
#ifndef CRPC_COROUTINE_COROUTINE_POOL_H_
#define CRPC_COROUTINE_COROUTINE_POOL_H_

#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/memory.h"

#include <vector>

namespace crpc {

class CoroutinePool {
public:
	CoroutinePool(int pool_size, int stack_size = 1024 * 128);
	~CoroutinePool();

	// 从池中获取一个空闲协程，如果没有空闲的则创建新的
	Coroutine::Ptr getCoroutineInstance();

	// 将使用完毕的协程归还到池中，标记为可分配
	void returnCoroutine(Coroutine::Ptr cor);

private:
	int pool_size_{0};	 // 协程池初始大小
	int stack_size_{0};	 // 每个协程的栈大小

	// 协程数组: first=协程指针, second=是否正在使用（true=占用, false=空闲）
	std::vector<std::pair<Coroutine::Ptr, bool>> free_cors_;

	Mutex mutex_;

	std::vector<Memory::Ptr> memory_pool_;	// 栈内存池
};

// 获取全局协程池单例（进程内唯一）
CoroutinePool* GetCoroutinePool();

}  // namespace crpc

#endif
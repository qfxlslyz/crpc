/**
 * 协程池
 * 预先创建一批常驻协程对象并复用，每个协程的栈内存由 Memory 对象统一管理
 *
 * 协程池采用两级复用策略：
 *   1. 初始 pool_size 个协程对象及其栈长期保留，通过 free_cors_ 复用。
 *   2. 初始协程全部占用时，按需创建临时 Coroutine 对象，仅将其栈内存
 *      从扩展 Memory 中分配并在使用后归还。临时对象不加入 free_cors_
 *
 * 因此 pool_size 是常驻协程数和栈内存的单次扩容粒度，不是协程总数上限
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

	// 优先复用常驻协程；如果全部占用，则利用扩展栈内存创建临时协程
	Coroutine::Ptr getCoroutineInstance();

	// 常驻协程归还对象槽位；临时协程只归还其栈内存
	void returnCoroutine(Coroutine::Ptr cor);

private:
	int pool_size_{0};	 // 常驻协程数，同时也是扩展 Memory 每批的 block 数
	int stack_size_{0};	 // 每个协程的栈大小

	// 仅保存初始常驻协程: first=协程指针, second=是否被占用
	std::vector<std::pair<Coroutine::Ptr, bool>> free_cors_;

	Mutex mutex_;

	// memory_pool_[0] 专属于常驻协程，后续 Memory 为临时协程提供可复用栈
	std::vector<Memory::Ptr> memory_pool_;
};

// 获取全局协程池单例（进程内唯一）
CoroutinePool* GetCoroutinePool();

}  // namespace crpc

#endif

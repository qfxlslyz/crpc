/**
 * 内存块管理器
 * 一次性分配一大块连续内存，然后按固定大小切分为多个 block，供协程栈使用
 * 通过 getBlock/backBlock 实现 block 的分配与回收，避免频繁的 malloc/free
 *
 * 内存布局: [block0][block1][block2]...[blockN-1]
 *           ^start_                              ^end_
 */
#ifndef CRPC_COROUTINE_MEMORY_H_
#define CRPC_COROUTINE_MEMORY_H_
#include "crpc/base/mutex.h"

#include <atomic>
#include <memory>
#include <vector>

namespace crpc {

class Memory {
public:
	using Ptr = std::shared_ptr<Memory>;

	Memory(int block_size, int block_count);

	~Memory();

	int getRefCount();

	char* getStart();

	char* getEnd();

	// 分配一个空闲 block，返回其起始地址；无空闲则返回 nullptr
	char* getBlock();

	// 回收一个 block，将其标记为空闲
	void backBlock(char* s);

	// 判断某个地址是否属于本 Memory 管理的内存范围
	bool hasBlock(char* s);

private:
	int block_size_{0};	  // 每个 block 的大小（即协程栈大小）
	int block_count_{0};  // block 总数

	int size_{0};			// 总内存大小 = block_size * block_count
	char* start_{nullptr};	// 内存起始地址
	char* end_{nullptr};	// 内存结束地址

	std::atomic<int> ref_counts_{0};  // 当前已分配的 block 数量
	std::vector<bool> blocks_;		  // 每个 block 的使用状态（true=已占用）
	Mutex mutex_;
};

}  // namespace crpc
#endif
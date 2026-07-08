#include "crpc/base/config.h"
#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_pool.h"

#include <sys/mman.h>
#include <vector>

namespace crpc {

extern Config::Ptr rpc_config;

static CoroutinePool* t_coroutine_container_ptr = nullptr;

CoroutinePool* GetCoroutinePool() {
	if (!t_coroutine_container_ptr) {
		t_coroutine_container_ptr =
			new CoroutinePool(rpc_config->cor_pool_size_, rpc_config->cor_stack_size_);
	}
	return t_coroutine_container_ptr;
}

CoroutinePool::CoroutinePool(int pool_size, int stack_size /*= 1024 * 128 B*/)
	: pool_size_(pool_size), stack_size_(stack_size) {
	// 先创建主协程
	Coroutine::getCurrentCoroutine();

	memory_pool_.push_back(std::make_shared<Memory>(stack_size, pool_size));

	Memory::Ptr tmp = memory_pool_[0];

	for (int i = 0; i < pool_size; ++i) {
		Coroutine::Ptr cor = std::make_shared<Coroutine>(stack_size, tmp->getBlock());
		cor->setIndex(i);
		free_cors_.push_back(std::make_pair(cor, false));
	}
}

CoroutinePool::~CoroutinePool() {}

Coroutine::Ptr CoroutinePool::getCoroutineInstance() {
	// 从头查找第一个空闲协程：1. it.second 为 false；2. getIsInCoFunc() 为
	// false 尽量复用曾经使用过的协程，避免优先选择从未使用过的协程
	// 因为已使用过的协程栈已经写入过物理内存，
	// 而未使用的协程只有 mmap 得到的虚拟地址，还没有真正写入物理内存
	// 因此 Linux 会在首次写入时分配物理页，并触发缺页中断

	Mutex::ScopedLock lock(mutex_);
	for (int i = 0; i < pool_size_; ++i) {
		if (!free_cors_[i].first->getIsInCoFunc() && !free_cors_[i].second) {
			free_cors_[i].second = true;
			Coroutine::Ptr cor = free_cors_[i].first;
			lock.unlock();
			return cor;
		}
	}

	for (size_t i = 1; i < memory_pool_.size(); ++i) {
		char* tmp = memory_pool_[i]->getBlock();
		if (tmp) {
			Coroutine::Ptr cor = std::make_shared<Coroutine>(stack_size_, tmp);
			return cor;
		}
	}
	memory_pool_.push_back(std::make_shared<Memory>(stack_size_, pool_size_));
	return std::make_shared<Coroutine>(stack_size_,
									   memory_pool_[memory_pool_.size() - 1]->getBlock());
}

void CoroutinePool::returnCoroutine(Coroutine::Ptr cor) {
	int i = cor->getIndex();
	if (i >= 0 && i < pool_size_) {
		free_cors_[i].second = false;
	} else {
		for (size_t i = 1; i < memory_pool_.size(); ++i) {
			if (memory_pool_[i]->hasBlock(cor->getStackPtr())) {
				memory_pool_[i]->backBlock(cor->getStackPtr());
			}
		}
	}
}

}  // namespace crpc

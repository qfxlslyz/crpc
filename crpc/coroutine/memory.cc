#include "crpc/base/log.h"
#include "crpc/coroutine/memory.h"

#include <assert.h>
#include <memory>
#include <stdlib.h>
#include <sys/mman.h>

namespace crpc {

// 一次性分配 block_count * block_size 字节的连续内存，初始化所有 block 为空闲
Memory::Memory(int block_size, int block_count)
	: block_size_(block_size), block_count_(block_count) {
	size_ = block_count_ * block_size_;
	start_ = (char*)malloc(size_);
	assert(start_ != (void*)-1);
	InfoLog << "succ mmap " << size_ << " bytes memory";
	end_ = start_ + size_ - 1;
	blocks_.resize(block_count_);
	for (size_t i = 0; i < blocks_.size(); ++i) {
		blocks_[i] = false;
	}
}

// void Memory::free() {
//   if (!start_ || start_ == (void*)-1) {
//     return;
//   }
//   int rt = free(start_);
//   if (rt != 0) {
//     ErrorLog << "munmap error, error=" << strerror(errno);
//   }
//   InfoLog << "~succ free munmap " << size_ << " bytes memory";
//   start_ = nullptr;
// }

Memory::~Memory() {
	if (!start_ || start_ == (void*)-1) {
		return;
	}
	free(start_);
	InfoLog << "~succ free munmap " << size_ << " bytes memory";
	start_ = nullptr;
}

char* Memory::getStart() {
	return start_;
}

char* Memory::getEnd() {
	return end_;
}

char* Memory::getBlock() {
	int t = -1;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		for (size_t i = 0; i < blocks_.size(); ++i) {
			if (blocks_[i] == false) {
				blocks_[i] = true;
				t = i;
				break;
			}
		}
	}
	
	if (t == -1) {
		return nullptr;
	}
	return start_ + (t * block_size_);
}

void Memory::backBlock(char* s) {
	if (s > end_ || s < start_) {
		ErrorLog << "error, this block is not belong to this Memory";
		return;
	}
	int i = (s - start_) / block_size_;
	std::lock_guard<std::mutex> lock(mutex_);
	blocks_[i] = false;
}

bool Memory::hasBlock(char* s) {
	return ((s >= start_) && (s <= end_));
}

}  // namespace crpc

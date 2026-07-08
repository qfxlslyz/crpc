#include "crpc/base/thread_pool.h"

#include <functional>
#include <pthread.h>
#include <queue>

namespace crpc {

void* ThreadPool::mainFunction(void* ptr) {
	ThreadPool* pool = reinterpret_cast<ThreadPool*>(ptr);
	pthread_cond_init(&pool->condition_, nullptr);

	while (!pool->is_stop_) {
		Mutex::ScopedLock lock(pool->mutex_);

		while (pool->tasks_.empty()) {
			pthread_cond_wait(&(pool->condition_), pool->mutex_.getMutex());
		}
		std::function<void()> cb = pool->tasks_.front();
		pool->tasks_.pop();
		lock.unlock();

		cb();
	}
	return nullptr;
}

ThreadPool::ThreadPool(int size) : size_(size) {
	for (int i = 0; i < size_; ++i) {
		pthread_t thread;
		threads_.emplace_back(thread);
	}
	pthread_cond_init(&condition_, nullptr);
}

void ThreadPool::start() {
	for (int i = 0; i < size_; ++i) {
		pthread_create(&threads_[i], nullptr, &ThreadPool::mainFunction, this);
	}
}

void ThreadPool::stop() {
	is_stop_ = true;
}

void ThreadPool::addTask(std::function<void()> cb) {
	Mutex::ScopedLock lock(mutex_);
	tasks_.push(cb);
	lock.unlock();
	pthread_cond_signal(&condition_);
}

ThreadPool::~ThreadPool() {
	// for (int i = 0; i < size_; ++i) {
	//   pthread_join(threads_[i], nullptr);
	// }
}

}  // namespace crpc

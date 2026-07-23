#include "crpc/base/thread_pool.h"

namespace crpc {

void ThreadPool::mainFunction() {
	while (true) {
		std::function<void()> cb;
		{
			std::unique_lock<std::mutex> lock(mutex_);
			condition_.wait(lock, [this]() { return is_stop_ || !tasks_.empty(); });
			if (is_stop_ && tasks_.empty()) {
				return;
			}
			cb = std::move(tasks_.front());
			tasks_.pop();
		}

		if (cb) {
			cb();
		}
	}
}

ThreadPool::ThreadPool(int size) : size_(size > 0 ? size : 0) {}

void ThreadPool::start() {
	std::lock_guard<std::mutex> lock(mutex_);
	if (is_started_ || is_stop_) {
		return;
	}
	is_started_ = true;
	threads_.reserve(size_);
	for (int i = 0; i < size_; ++i) {
		threads_.emplace_back(&ThreadPool::mainFunction, this);
	}
}

void ThreadPool::stop() {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (is_stop_) {
			return;
		}
		is_stop_ = true;
	}
	condition_.notify_all();

	for (std::thread& thread : threads_) {
		if (thread.joinable()) {
			thread.join();
		}
	}
}

void ThreadPool::addTask(std::function<void()> cb) {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (is_stop_) {
			return;
		}
		tasks_.push(std::move(cb));
	}
	condition_.notify_one();
}

ThreadPool::~ThreadPool() {
	stop();
}

}  // namespace crpc

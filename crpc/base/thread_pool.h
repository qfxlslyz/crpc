/**
 * 通用线程池
 * 维护一组工作线程，通过任务队列分配工作
 * 使用互斥锁 + 条件变量实现线程安全的生产者-消费者模型
 */
#ifndef CRPC_BASE_THREAD_POOL_H_
#define CRPC_BASE_THREAD_POOL_H_

#include "crpc/base/mutex.h"

#include <functional>
#include <pthread.h>
#include <queue>

namespace crpc {

class ThreadPool {
public:
	ThreadPool(int size);

	~ThreadPool();

	void start();

	void stop();

	// 向任务队列中添加一个任务，并通知一个空闲的工作线程
	void addTask(std::function<void()> cb);

private:
	// 工作线程的入口函数，循环从队列中取任务执行
	static void* mainFunction(void* ptr);

public:
	int size_{0};							   // 线程数量
	std::vector<pthread_t> threads_;		   // 线程句柄数组
	std::queue<std::function<void()>> tasks_;  // 任务队列

	Mutex mutex_;				// 保护任务队列的互斥锁
	pthread_cond_t condition_;	// 通知工作线程的条件变量
	bool is_stop_{false};
};

}  // namespace crpc

#endif
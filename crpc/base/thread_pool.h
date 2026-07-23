/**
 * 通用线程池
 * 维护一组工作线程，通过任务队列分配工作
 * 使用互斥锁 + 条件变量实现线程安全的生产者-消费者模型
 */
#ifndef CRPC_BASE_THREAD_POOL_H_
#define CRPC_BASE_THREAD_POOL_H_

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace crpc {

class ThreadPool {
public:
	ThreadPool(int size);

	~ThreadPool();

	void start();

	// stop() 会处理完已经进入队列的任务，然后等待全部线程退出
	void stop();

	// 向任务队列中添加一个任务，并通知一个空闲的工作线程
	void addTask(std::function<void()> cb);

private:
	// 工作线程的入口函数，循环从队列中取任务执行
	void mainFunction();

	int size_{0};							   // 线程数量
	std::vector<std::thread> threads_;		   // 工作线程数组
	std::queue<std::function<void()>> tasks_;  // 任务队列

	std::mutex mutex_;					 // 保护任务队列和停止状态
	std::condition_variable condition_;	 // 通知工作线程有任务或线程池停止
	bool is_stop_{false};
	bool is_started_{false};
};

}  // namespace crpc

#endif

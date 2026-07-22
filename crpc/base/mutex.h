/**
 * 互斥锁模块
 * 封装 pthread 互斥锁和读写锁，提供 RAII 风格的 ScopedLock
 */
#ifndef CRPC_BASE_MUTEX_H_
#define CRPC_BASE_MUTEX_H_

#include <pthread.h>

namespace crpc {

template <class T>
struct ScopedLockImpl {
public:
	explicit ScopedLockImpl(T& mutex) : mutex_(mutex) {
		mutex_.lock();
		locked_ = true;
	}

	~ScopedLockImpl() { unlock(); }

	void lock() {
		if (!locked_) {
			mutex_.lock();
			locked_ = true;
		}
	}

	void unlock() {
		if (locked_) {
			mutex_.unlock();
			locked_ = false;
		}
	}

private:
	T& mutex_;
	bool locked_{false};
};

template <class T>
struct ReadScopedLockImpl {
public:
	explicit ReadScopedLockImpl(T& mutex) : mutex_(mutex) {
		mutex_.rdLock();
		locked_ = true;
	}

	~ReadScopedLockImpl() { unlock(); }

	void lock() {
		if (!locked_) {
			mutex_.rdLock();
			locked_ = true;
		}
	}

	void unlock() {
		if (locked_) {
			mutex_.unlock();
			locked_ = false;
		}
	}

private:
	T& mutex_;
	bool locked_{false};
};

/**
 * @brief 局部写锁模板实现
 */
template <class T>
struct WriteScopedLockImpl {
public:
	explicit WriteScopedLockImpl(T& mutex) : mutex_(mutex) {
		mutex_.wrLock();
		locked_ = true;
	}

	~WriteScopedLockImpl() { unlock(); }

	void lock() {
		if (!locked_) {
			mutex_.wrLock();
			locked_ = true;
		}
	}

	void unlock() {
		if (locked_) {
			mutex_.unlock();
			locked_ = false;
		}
	}

private:
	T& mutex_;
	bool locked_{false};
};

class Mutex {
public:
	using ScopedLock = ScopedLockImpl<Mutex>;

	Mutex() { pthread_mutex_init(&mutex_, nullptr); }

	~Mutex() { pthread_mutex_destroy(&mutex_); }

	void lock() { pthread_mutex_lock(&mutex_); }

	void unlock() { pthread_mutex_unlock(&mutex_); }

	pthread_mutex_t* getMutex() { return &mutex_; }

private:
	pthread_mutex_t mutex_;
};

class RWMutex {
public:
	using ReadScopedLock = ReadScopedLockImpl<RWMutex>;

	using WriteScopedLock = WriteScopedLockImpl<RWMutex>;

	RWMutex() { pthread_rwlock_init(&lock_, nullptr); }

	~RWMutex() { pthread_rwlock_destroy(&lock_); }

	void rdLock() { pthread_rwlock_rdlock(&lock_); }

	void wrLock() { pthread_rwlock_wrlock(&lock_); }

	void unlock() { pthread_rwlock_unlock(&lock_); }

private:
	pthread_rwlock_t lock_;
};

}  // namespace crpc
#endif

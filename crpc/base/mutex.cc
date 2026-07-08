#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/event/reactor.h"

#include <memory>
#include <pthread.h>

namespace crpc {

// 本文件参考 sylar 的实现

CoroutineMutex::CoroutineMutex() {}

CoroutineMutex::~CoroutineMutex() {
	if (lock_) {
		unlock();
	}
}

void CoroutineMutex::lock() {
	if (Coroutine::isMainCoroutine()) {
		ErrorLog << "main coroutine can't use coroutine mutex";
		return;
	}

	Coroutine* cor = Coroutine::getCurrentCoroutine();

	Mutex::ScopedLock lock(mutex_);
	if (!lock_) {
		lock_ = true;
		DebugLog << "coroutine succ get coroutine mutex";
		lock.unlock();
	} else {
		sleep_cors_.push(cor);
		auto tmp = sleep_cors_;
		lock.unlock();

		DebugLog << "coroutine yield, pending coroutine mutex, current sleep queue "
					"exist ["
				 << tmp.size() << "] coroutines";

		Coroutine::Yield();
	}
}

void CoroutineMutex::unlock() {
	if (Coroutine::isMainCoroutine()) {
		ErrorLog << "main coroutine can't use coroutine mutex";
		return;
	}

	Mutex::ScopedLock lock(mutex_);
	if (lock_) {
		lock_ = false;
		if (sleep_cors_.empty()) {
			return;
		}

		Coroutine* cor = sleep_cors_.front();
		sleep_cors_.pop();
		lock.unlock();

		if (cor) {
			// 唤醒等待队列中的第一个协程
			DebugLog << "coroutine unlock, now to resume coroutine[" << cor->getCorId() << "]";

			Reactor::getReactor()->addTask([cor]() { Coroutine::Resume(cor); }, true);
		}
	}
}

}  // namespace crpc

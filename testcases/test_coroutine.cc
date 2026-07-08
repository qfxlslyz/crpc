#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/coroutine/coroutine_pool.h"

#include <iostream>
#include <pthread.h>

#include <google/protobuf/service.h>

crpc::Coroutine::Ptr cor;
crpc::Coroutine::Ptr cor2;

class Test {
public:
	crpc::CoroutineMutex coroutine_mutex_;
	int a = 1;
};
Test test_;

void fun1() {
	std::cout << "cor1 ---- now fitst resume fun1 coroutine by thread 1" << std::endl;
	std::cout << "cor1 ---- now begin to yield fun1 coroutine" << std::endl;

	test_.coroutine_mutex_.lock();

	std::cout << "cor1 ---- coroutine lock on test_, sleep 5s begin" << std::endl;

	sleep(5);
	std::cout << "cor1 ---- sleep 5s end, now back coroutine lock" << std::endl;

	test_.coroutine_mutex_.unlock();

	crpc::Coroutine::Yield();
	std::cout << "cor1 ---- fun1 coroutine back, now end" << std::endl;
}

void fun2() {
	std::cout << "cor222 ---- now fitst resume fun1 coroutine by thread 1" << std::endl;
	std::cout << "cor222 ---- now begin to yield fun1 coroutine" << std::endl;

	sleep(2);
	std::cout << "cor222 ---- coroutine2 want to get coroutine lock of test_" << std::endl;
	test_.coroutine_mutex_.lock();

	std::cout << "cor222 ---- coroutine2 get coroutine lock of test_ succ" << std::endl;
}

void* thread1_func(void*) {
	std::cout << "thread 1 begin" << std::endl;
	std::cout << "now begin to resume fun1 coroutine in thread 1" << std::endl;
	crpc::Coroutine::Resume(cor.get());
	std::cout << "now fun1 coroutine back in thread 1" << std::endl;
	std::cout << "thread 1 end" << std::endl;
	return nullptr;
}

void* thread2_func(void*) {
	crpc::Coroutine::getCurrentCoroutine();
	std::cout << "thread 2 begin" << std::endl;
	std::cout << "now begin to resume fun1 coroutine in thread 2" << std::endl;
	crpc::Coroutine::Resume(cor2.get());
	std::cout << "now fun1 coroutine back in thread 2" << std::endl;
	std::cout << "thread 2 end" << std::endl;
	return nullptr;
}

int main(int argc, char* argv[]) {
	crpc::SetHook(false);
	std::cout << "main begin" << std::endl;
	int stack_size = 128 * 1024;
	char* sp = reinterpret_cast<char*>(malloc(stack_size));
	cor = std::make_shared<crpc::Coroutine>(stack_size, sp, fun1);

	char* sp2 = reinterpret_cast<char*>(malloc(stack_size));
	cor2 = std::make_shared<crpc::Coroutine>(stack_size, sp2, fun2);

	pthread_t thread2;
	pthread_create(&thread2, nullptr, &thread2_func, nullptr);

	thread1_func(nullptr);

	pthread_join(thread2, nullptr);

	std::cout << "main end" << std::endl;
}

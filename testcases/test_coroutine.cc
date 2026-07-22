#include "crpc/coroutine/coroutine.h"

#include <cassert>
#include <iostream>
#include <memory>
#include <vector>

int main() {
	const int stack_size = 128 * 1024;
	std::unique_ptr<char[]> stack1(new char[stack_size]);
	std::unique_ptr<char[]> stack2(new char[stack_size]);
	std::vector<int> execution_order;

	crpc::Coroutine::Ptr cor1 = std::make_shared<crpc::Coroutine>(
		stack_size, stack1.get(), [&execution_order]() {
			execution_order.push_back(1);
			crpc::Coroutine::Yield();
			execution_order.push_back(3);
		});
	crpc::Coroutine::Ptr cor2 = std::make_shared<crpc::Coroutine>(
		stack_size, stack2.get(), [&execution_order]() {
			execution_order.push_back(2);
			crpc::Coroutine::Yield();
			execution_order.push_back(4);
		});

	crpc::Coroutine::Resume(cor1.get());
	crpc::Coroutine::Resume(cor2.get());
	crpc::Coroutine::Resume(cor1.get());
	crpc::Coroutine::Resume(cor2.get());

	const std::vector<int> expected{1, 2, 3, 4};
	assert(execution_order == expected);
	std::cout << "coroutine scheduling test passed" << std::endl;
	return 0;
}

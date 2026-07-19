#include "crpc/net/transport/net_address.h"
#include "crpc/rpc/crpc_async_channel.h"
#include "crpc/rpc/crpc_controller.h"
#include "crpc/rpc/rpc_closure.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdlib.h>

#include <google/protobuf/service.h>

#include "test_rpc_server.pb.h"

int main(int argc, char* argv[]) {
	const char* ip = "127.0.0.1";
	int port = 20000;
	if (argc >= 3) {
		ip = argv[1];
		port = std::atoi(argv[2]);
	} else if (argc == 2) {
		port = std::atoi(argv[1]);
	}

	auto addr = std::make_shared<crpc::IPAddress>(ip, port);
	auto controller = std::make_shared<crpc::CrpcController>();
	controller->setTimeout(5000);
	auto request = std::make_shared<queryAgeReq>();
	auto response = std::make_shared<queryAgeRes>();
	request->set_req_no(1);
	request->set_id(100);

	auto closure_called = std::make_shared<std::atomic<bool>>(false);
	auto closure =
		std::make_shared<crpc::RpcClosure>([closure_called]() { closure_called->store(true); });
	auto async_channel = std::make_shared<crpc::CrpcAsyncChannel>(addr);
	async_channel->saveCallee(controller, request, response, closure);
	QueryService_Stub stub(async_channel.get());

	std::cout << "Send async request to " << addr->toString()
			  << ", request body: " << request->ShortDebugString() << std::endl;
	auto call_begin = std::chrono::steady_clock::now();
	stub.query_age(controller.get(), request.get(), response.get(), nullptr);
	auto call_return = std::chrono::steady_clock::now();
	auto call_method_ms =
		std::chrono::duration_cast<std::chrono::milliseconds>(call_return - call_begin).count();

	std::cout << "CallMethod returned in " << call_method_ms
			  << " ms; the client main thread can continue working" << std::endl;
	if (call_method_ms >= 500) {
		std::cerr << "CallMethod did not return asynchronously" << std::endl;
		return 1;
	}

	async_channel->wait();
	if (controller->errorCode() != 0) {
		std::cerr << "Async RPC failed, error code: " << controller->errorCode()
				  << ", error info: " << controller->ErrorText() << std::endl;
		return 1;
	}
	if (!closure_called->load()) {
		std::cerr << "Async RPC closure was not called before wait() returned" << std::endl;
		return 1;
	}

	std::cout << "Async RPC succeeded, response body: " << response->ShortDebugString()
			  << std::endl;
	return 0;
}

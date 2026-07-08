#include "crpc/net/transport/net_address.h"
#include "crpc/rpc/crpc_channel.h"
#include "crpc/rpc/crpc_controller.h"
#include "crpc/rpc/rpc_closure.h"

#include <iostream>
#include <stdlib.h>
#include <string.h>

#include <google/protobuf/service.h>

#include "test_rpc_server.pb.h"

void test_client(const char* ip, int port) {
	crpc::IPAddress::Ptr addr = std::make_shared<crpc::IPAddress>(ip, port);

	crpc::CrpcChannel channel(addr);
	QueryService_Stub stub(&channel);

	crpc::CrpcController rpc_controller;
	rpc_controller.setTimeout(5000);

	queryAgeReq rpc_req;
	queryAgeRes rpc_res;

	rpc_req.set_req_no(1);
	rpc_req.set_id(100);

	std::cout << "Send to crpc server " << addr->toString()
			  << ", request body: " << rpc_req.ShortDebugString() << std::endl;
	stub.query_age(&rpc_controller, &rpc_req, &rpc_res, nullptr);

	if (rpc_controller.errorCode() != 0) {
		std::cout << "Failed to call crpc server, error code: " << rpc_controller.errorCode()
				  << ", error info: " << rpc_controller.ErrorText() << std::endl;
		return;
	}

	std::cout << "Success get response from crpc server " << addr->toString()
			  << ", response body: " << rpc_res.ShortDebugString() << std::endl;
}

int main(int argc, char* argv[]) {
	const char* ip = "127.0.0.1";
	int port = 20000;

	if (argc >= 3) {
		ip = argv[1];
		port = std::atoi(argv[2]);
	} else if (argc == 2) {
		port = std::atoi(argv[1]);
	}
	std::cout << "ip: " << ip << ", port: " << port << std::endl;
	test_client(ip, port);

	return 0;
}

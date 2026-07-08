#include "crpc/base/log.h"
#include "crpc/base/mutex.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_server.h"
#include "crpc/rpc/rpc_server.h"

#include <atomic>
#include <sstream>

#include <google/protobuf/service.h>

#include "test_rpc_server.pb.h"

static int i = 0;
crpc::CoroutineMutex g_cor_mutex;

class QueryServiceImpl : public QueryService {
public:
	QueryServiceImpl() {}
	~QueryServiceImpl() {}

	void query_name(google::protobuf::RpcController* controller, const ::queryNameReq* request,
					::queryNameRes* response, ::google::protobuf::Closure* done) {
		AppInfoLog("QueryServiceImpl.query_name, req={%s}", request->ShortDebugString().c_str());
		response->set_id(request->id());
		response->set_name("ikerli");

		AppInfoLog("QueryServiceImpl.query_name, res={%s}", response->ShortDebugString().c_str());

		if (done) {
			done->Run();
		}
	}

	void query_age(google::protobuf::RpcController* controller, const ::queryAgeReq* request,
				   ::queryAgeRes* response, ::google::protobuf::Closure* done) {
		AppInfoLog("QueryServiceImpl.query_age, req={%s}", request->ShortDebugString().c_str());
		// AppInfoLog << "QueryServiceImpl.query_age, sleep 6 s begin";
		// sleep(6);
		// AppInfoLog << "QueryServiceImpl.query_age, sleep 6 s end";

		response->set_ret_code(0);
		response->set_res_info("OK");
		response->set_req_no(request->req_no());
		response->set_id(request->id());
		response->set_age(100100111);

		// g_cor_mutex.lock();
		AppDebugLog("begin i = %d", i);
		sleep(1);
		i++;
		AppDebugLog("end i = %d", i);
		// g_cor_mutex.unlock();

		if (done) {
			done->Run();
		}
		// printf("response = %s\n", response->ShortDebugString().c_str());

		AppInfoLog("QueryServiceImpl.query_age, res={%s}", response->ShortDebugString().c_str());
	}
};

int main(int argc, char* argv[]) {
	if (argc != 2) {
		printf("Start CRPC server error, input argc is not 2!");
		printf("Start CRPC server like this: \n");
		printf("./server a.xml\n");
		return 0;
	}

	crpc::InitConfig(argv[1]);

	REGISTER_SERVICE(QueryServiceImpl);

	crpc::StartRpcServer();

	return 0;
}

#include "crpc/base/config.h"
#include "crpc/base/log.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/net/transport/net_address.h"
#include "crpc/net/transport/tcp_server.h"
#include "crpc/rpc/rpc_codec.h"
#include "crpc/rpc/rpc_dispatcher.h"
#include "crpc/rpc/rpc_server.h"

#include <google/protobuf/service.h>

namespace crpc {

Config::Ptr rpc_config;
Logger::Ptr rpc_logger;
TcpServer::Ptr rpc_server;
std::shared_ptr<RpcDispatcher> rpc_dispatcher;

static int g_init_config = 0;

void InitConfig(const char* file) {
	SetHook(false);

#ifdef DECLARE_MYSQL_PLUGIN
	int rt = mysql_library_init(0, nullptr, nullptr);
	if (rt != 0) {
		printf("Start CRPC server error, call mysql_library_init error\n");
		mysql_library_end();
		exit(0);
	}
#endif

	SetHook(true);

	if (g_init_config == 0) {
		rpc_config = std::make_shared<Config>(file);
		rpc_config->readConf();
		NetAddress::Ptr addr =
			std::make_shared<IPAddress>(rpc_config->server_ip_, rpc_config->server_port_);
		rpc_dispatcher = std::make_shared<RpcDispatcher>();
		Codec::Ptr codec = std::make_shared<RpcCodec>();
		rpc_server = std::make_shared<TcpServer>(addr, rpc_dispatcher, codec, kDefaultRpcProtocol);
		g_init_config = 1;
	}
}

bool RegisterService(std::shared_ptr<google::protobuf::Service> service) {
	if (!service) {
		ErrorLog << "register service error, service ptr is nullptr";
		return false;
	}
	if (!rpc_dispatcher) {
		ErrorLog << "register service error, rpc dispatcher is nullptr";
		return false;
	}
	rpc_dispatcher->registerService(service);
	return true;
}

void StartRpcServer() {
	rpc_logger->start();
	rpc_server->start();
}

}  // namespace crpc

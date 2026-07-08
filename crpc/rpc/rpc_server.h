/**
 * 框架启动入口
 * 提供 CRPC 服务器的初始化、服务注册和启动的顶层 API
 *
 * 典型使用流程:
 *   1. InitConfig("conf/server.xml");        // 读取配置文件
 *   2. REGISTER_SERVICE(MyServiceImpl);       // 注册 Protobuf 服务
 *   3. StartRpcServer();                      // 启动服务器（阻塞）
 */
#ifndef CRPC_RPC_RPC_SERVER_H_
#define CRPC_RPC_RPC_SERVER_H_

#include "crpc/base/log.h"

#include <memory>
#include <stdio.h>

#include <google/protobuf/service.h>

namespace crpc {

// 注册 Protobuf Service 的便捷宏，注册失败时自动退出
#define REGISTER_SERVICE(service)                                             \
	do {                                                                      \
		if (!crpc::RegisterService(std::make_shared<service>())) {            \
			printf(                                                           \
				"Start CRPC server error, because register protobuf service " \
				"error, "                                                     \
				"please look up rpc log get more details!\n");                \
			crpc::Exit(0);                                                    \
		}                                                                     \
	} while (0)

// 初始化全局配置，解析 XML 配置文件并创建 Logger 和 TcpServer
void InitConfig(const char* file);

// 启动 RPC 服务器（阻塞当前线程，进入事件循环）
void StartRpcServer();

// 注册 Protobuf RPC 服务
bool RegisterService(std::shared_ptr<google::protobuf::Service> service);

}  // namespace crpc
#endif

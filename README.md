# CRPC

CRPC（Coroutine RPC）是一个基于 C++11 的轻量级协程 RPC 框架。它使用 Reactor、epoll、用户态协程、Protobuf 和内置二进制编解码构建核心 RPC 链路，对外提供同步编码风格，底层在等待 socket IO 时自动让出当前协程，让线程继续处理其他事件。

这个项目聚焦 RPC 框架的核心机制，适合学习、实验和阅读源码。当前实现刻意保持较小的功能边界，方便理解网络模型、协程调度和 RPC 分发流程。

## 特性

- 基于 epoll 的 Reactor 网络模型，使用 eventfd 唤醒事件循环。
- 用户态协程封装，支持上下文切换、协程池和协程级 IO 等待。
- socket hook 将阻塞式读写转换为协程让出和恢复。
- 内置二进制 RPC 编解码处理 TCP 粘包和拆包。
- 基于 Protobuf Service 完成服务注册、方法查找、请求分发和响应编码。
- 客户端保持同步调用写法，等待响应时当前协程 Yield。
- TimerEvent 支持连接、发送、接收和空闲连接超时控制。
- 内置 RPC 服务端、客户端和协程调度示例。

## 架构概览

```text
client stub
   |
   v
CrpcChannel
   |
   v
TcpClient / TcpConnection
   |
   v
Reactor + epoll + coroutine hook
   |
   v
TcpServer / IOThread
   |
   v
RpcDispatcher
   |
   v
google::protobuf::Service
```

服务端启动后读取 XML 配置，创建 TcpServer、主 Reactor 和 IOThread。主 Reactor 负责 accept，新连接分发给 IO 线程；TcpConnection 负责读写缓冲区、内部 RPC 编解码和请求响应；RpcDispatcher 根据 `service_full_name` 查找 Protobuf Service 和 Method 并调用业务实现。

客户端通过 CrpcChannel 发起调用，调用代码看起来是同步的；当连接、发送或接收需要等待 IO 就绪时，当前协程让出执行权，底层 Reactor 监听到事件后再恢复该协程。

## 环境要求

- Linux
- C++11 编译器
- CMake 3.0+
- Protobuf
- tinyxml
- pthread / dl

Ubuntu/Debian 可参考：

```bash
sudo apt install build-essential cmake protobuf-compiler libprotobuf-dev libtinyxml-dev
```

## 构建

```bash
git clone <repo-url>
cd <repo-dir>
./build.sh
```

构建产物：

```text
lib/libcrpc.a
bin/test_coroutine
bin/test_rpc_server
bin/test_rpc_server_client
```

也可以直接使用 CMake：

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
```

MySQL 插件默认不参与核心库构建；需要启用时使用：

```bash
cmake -S . -B build -DCRPC_ENABLE_MYSQL=ON
cmake --build build -j$(nproc)
```

## 快速开始

启动示例服务端：

```bash
./bin/test_rpc_server conf/test_rpc_server.xml
```

另开一个终端启动示例客户端：

```bash
./bin/test_rpc_server_client 127.0.0.1 20000
```

运行协程调度示例：

```bash
./bin/test_coroutine
```

## 配置说明

示例配置文件位于 [conf/test_rpc_server.xml](conf/test_rpc_server.xml)，主要配置项：

- `server.ip` / `server.port`：服务监听地址。
- `server.protocol`：协议类型，当前默认使用内置二进制 RPC 编解码（配置值为 CRPC）。
- `iothread_num`：IO 线程数量。
- `coroutine.coroutine_stack_size`：协程栈大小，单位 KB。
- `coroutine.coroutine_pool_size`：默认协程池大小。
- `max_connect_timeout`：客户端连接超时时间，单位秒。
- `time_wheel.bucket_num` / `time_wheel.interval`：空闲连接清理相关参数。
- `rpc_log_level` / `app_log_level`：RPC 日志和应用日志级别。

## 目录结构

```text
.
├── CMakeLists.txt
├── build.sh
├── conf/
│   └── test_rpc_server.xml
├── crpc/
│   ├── base/          # 日志、配置、锁、线程池、错误码、消息序号和通用工具
│   ├── coroutine/     # 协程、协程池、hook、上下文切换
│   ├── net/
│   │   ├── event/     # Reactor、FdEvent、Timer 等事件驱动基础设施
│   │   ├── protocol/  # 协议数据、编解码和分发抽象
│   │   └── transport/ # TcpServer、TcpClient、TcpConnection、IOThread、地址和缓冲区
│   ├── rpc/           # Channel、Controller、Dispatcher、编解码和服务启动入口
│   └── plugins/
│       └── mysql/     # 可选 MySQL 集成
├── testcases/         # 示例服务端、客户端、proto 和协程测试
└── LICENSE
```

## 当前边界

- 当前仅内置一套基于 Protobuf 的二进制 RPC 编解码实现。
- 当前网络模型依赖 Linux epoll、eventfd 等能力。
- MySQL 相关代码是可选能力，需要通过 `CRPC_ENABLE_MYSQL` 构建选项启用。
- 项目暂不包含服务治理、注册中心、负载均衡等生产级功能。

## License

CRPC 使用 Apache License 2.0 开源协议，详情见 [LICENSE](LICENSE)。

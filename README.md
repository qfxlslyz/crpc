# CRPC

CRPC（Coroutine RPC）是一个基于 C++17 的轻量级协程 RPC 框架。它使用 Reactor、epoll、用户态协程、Protobuf 和内置二进制编解码构建核心 RPC 链路，对外同时提供同步和异步 RPC Channel，底层在等待 socket IO 时自动让出当前协程，让线程继续处理其他事件。

CRPC 采用多线程 Reactor 与用户态协程结合的协作式 M:N 模型。连接协程固定由所属 IO 线程的 SubReactor 调度，当前不支持跨 IO 线程的协程迁移和工作窃取。

这个项目聚焦 RPC 框架的核心机制，适合学习、实验和阅读源码。当前实现刻意保持较小的功能边界，方便理解网络模型、协程调度和 RPC 分发流程。

## 特性

- 基于 epoll 的 Reactor 网络模型，使用 eventfd 唤醒事件循环。
- 用户态协程封装，支持上下文切换、协程池和协程级 IO 等待。
- socket hook 将阻塞式读写转换为协程让出和恢复。
- 内置二进制 RPC 编解码处理 TCP 粘包和拆包。
- 基于 Protobuf Service 完成服务注册、方法查找、请求分发和响应编码。
- `CrpcChannel` 提供同步调用写法，等待响应时当前协程 Yield。
- `CrpcAsyncChannel` 支持独立客户端直接发起异步 RPC，实际调用由共享 IO 线程中的工作协程执行。
- 异步调用支持 Closure 完成回调以及 `wait()` 等待结果。
- TimerEvent 支持连接、发送、接收和空闲连接超时控制。
- 内置 RPC 服务端、客户端和协程调度示例。

## 架构概览

客户端的 Protobuf Stub 通过 `google::protobuf::RpcChannel` 发起调用。`CrpcChannel` 在当前协程中同步执行完整 RPC；`CrpcAsyncChannel` 本身不重复实现网络链路，而是先把任务投递到进程共享的 `AsyncClientRuntime`，再由其 IO 线程中的工作协程调用 `CrpcChannel`。

```text
Protobuf Stub
    |
    +-- CrpcChannel ---------------------------------------------+
    |   （同步语义：响应就绪后 CallMethod 返回）             |
    |                                                            |
    +-- CrpcAsyncChannel                                         |
        （异步语义：投递任务后 CallMethod 返回）             |
             |                                                   |
             v                                                   |
        AsyncClientRuntime（进程内共享）                         |
          ├─ 1 个 IOThread + 1 个 Reactor                         |
          └─ CoroutinePool -> 工作协程 -> CrpcChannel --------+
                                                                  |
                                                                  v
                                      RpcCodec -> TcpClient -> TcpConnection
                                                                  |
                                               connect/read/write coroutine hook
                                                                  |
                                     ========== CRPC/TCP ==========
                                                                  |
                                                                  v
TcpAcceptor -> MainReactor + accept 协程
                              |
                              | 轮询分配已接受连接
                              v
                         IOThreadPool
                              |
                              v
                IOThread + SubReactor（每个 IO 线程一个）
                              |
                              v
            TcpConnection + 连接协程（每个服务端连接一个）
                              |
                 input -> RpcCodec::decode
                              |
                              v
                        RpcDispatcher
                              |
             按 service_full_name 查找 Service/Method
                              |
                              v
              google::protobuf::Service::CallMethod
                              |
                RpcCodec::encode -> output -> TCP
```

客户端每次 `CrpcChannel` 调用都会创建本次请求使用的 `RpcCodec`、`TcpClient` 和客户端 `TcpConnection`，将 Protobuf 请求封装成 `RpcMessage` 后编码、发送，再读取响应并反序列化到业务 Response。异步 Channel 只改变调用在哪个线程/协程中执行以及完成结果如何通知，与同步 Channel 共用同一套 `CrpcChannel -> TcpClient -> TcpConnection` 网络实现。

服务端的 `InitConfig()` 读取 XML，创建 `RpcDispatcher`、`RpcCodec`、`TcpServer`、MainReactor 和 `IOThreadPool`。MainReactor 运行 accept 协程并处理服务端级定时任务；新连接按轮询分配到某个 IO 线程，之后固定由该线程的 SubReactor 驱动。每个服务端 `TcpConnection` 拥有一个连接协程，按 `input -> decode -> dispatch -> encode -> output` 处理请求和响应。`RpcDispatcher` 使用 `service_full_name` 定位已注册的 Protobuf Service 及 Method，并同步调用业务实现。

客户端和服务端的 socket 等待都由 coroutine hook 与当前线程的 Reactor 协作：IO 暂未就绪时注册 epoll 事件并 `Yield` 当前子协程，事件就绪后由 Reactor 恢复对应协程。独立客户端使用 `CrpcAsyncChannel` 时无需初始化 RPC Server；首次有效异步调用会按需创建全进程唯一的 `AsyncClientRuntime`，后续异步 RPC 复用其 IO 线程、Reactor 和协程池。

## 协程模型

当前项目采用“每个线程一个主协程 + 多个子协程”的非对称协程模型。主协程不是业务入口的 `main()` 函数，而是当前线程中的调度上下文；子协程用于执行 accept、连接读写、RPC 调用等具体任务。

```text
主协程
  -> Resume 子协程
子协程执行 IO
  -> 没有就绪时 Yield
  -> 切回主协程
主协程继续跑 Reactor::loop()
  -> epoll_wait 等事件
事件就绪
  -> 再 Resume 对应子协程
```

因此，业务和 RPC 调用代码可以保持同步写法；当 `accept`、`read`、`write`、`connect` 或 `sleep` 需要等待时，hook 层会把 fd 注册到 Reactor，并让当前子协程 `Yield`。主协程继续运行事件循环，等 epoll 通知 IO 就绪后再恢复对应子协程。

### 协程池复用策略

`CoroutinePool` 采用“常驻协程对象 + 可扩展栈内存”的两级复用策略：

- 创建协程池时，预先创建 `pool_size` 个常驻 `Coroutine` 对象及对应的栈。这些对象保存在 `free_cors_` 中，使用完毕后通过更新占用标记反复复用。
- 常驻协程全部占用时，协程池会从扩展 `Memory` 中分配栈，按需创建临时 `Coroutine` 对象。临时对象不加入 `free_cors_`。
- 临时协程使用完毕后，其栈 block 会归还给扩展 `Memory`；`Coroutine` 对象在最后一个 `shared_ptr` 释放后销毁。
- 扩展 `Memory` 也没有空闲栈时，再以 `pool_size` 个栈 block 为一批扩容。

因此，`pool_size` 表示常驻协程对象的数量，同时也是栈内存的单次扩容粒度；它不是协程总数或并发数的硬上限。

## RPC 调用方式

### 同步调用

`CrpcChannel::CallMethod()` 会在获得响应后返回。调用发生在协程中时，等待网络 IO 只会挂起当前协程。

```cpp
crpc::CrpcChannel channel(addr);
QueryService_Stub stub(&channel);

crpc::CrpcController controller;
queryAgeReq request;
queryAgeRes response;

stub.query_age(&controller, &request, &response, nullptr);
```

### 独立客户端异步调用

独立客户端可以直接构造 `CrpcAsyncChannel`，不需要调用 `InitConfig()` 或启动 `TcpServer`。`CallMethod()` 只投递异步任务并立即返回，客户端线程可以继续处理其他工作；需要结果时再调用 `wait()`。

异步执行环境按需创建，并由进程内的所有 `CrpcAsyncChannel` 共享：

```text
CrpcAsyncChannel A ─┐
CrpcAsyncChannel B ─┼──> AsyncClientRuntime
CrpcAsyncChannel C ─┘       ├── 1 个 IOThread
                              ├── 1 个 Reactor
                              └── 1 个 CoroutinePool
```

- 只构造 `CrpcAsyncChannel` 不会创建线程。
- 第一次有效调用 `CallMethod()` 时，进程会创建唯一的 `AsyncClientRuntime` 和其异步 IO 线程。
- 后续 Channel 和异步 RPC 复用同一运行时；并发调用增加的是工作协程数量，而不是线程数量。

因此，对于只有一个主线程的独立客户端，首次异步调用后通常共有两个线程：客户端主线程和共享异步 IO 线程。

```cpp
auto addr = std::make_shared<crpc::IPAddress>("127.0.0.1", 20000);
auto controller = std::make_shared<crpc::CrpcController>();
auto request = std::make_shared<queryAgeReq>();
auto response = std::make_shared<queryAgeRes>();

request->set_req_no(1);
request->set_id(100);
controller->setTimeout(5000);

auto closure = std::make_shared<crpc::RpcClosure>([response]() {
	std::cout << "RPC completed: " << response->ShortDebugString() << std::endl;
});
auto channel = std::make_shared<crpc::CrpcAsyncChannel>(addr);

// 异步执行期间由 Channel 保持这些对象的 shared_ptr 引用
channel->saveCallee(controller, request, response, closure);

QueryService_Stub stub(channel.get());
stub.query_age(controller.get(), request.get(), response.get(), nullptr);

// CallMethod 已返回，可以执行不依赖 RPC 结果的工作
doSomethingElse();

channel->wait();
if (controller->errorCode() != 0) {
	std::cerr << controller->ErrorText() << std::endl;
}
```

异步调用有以下约束：

- Channel、Controller、请求、响应和 Closure 应使用 `std::shared_ptr` 管理。
- 必须在调用 protobuf Stub 前执行 `saveCallee()`；Closure 可以传入空指针。
- 独立客户端中的 Closure 在异步 IO 线程执行，访问其他共享状态时需要自行同步。
- 独立客户端调用 `wait()` 会阻塞当前普通线程，但不会阻塞异步 IO 线程。
- 在服务端 IO 协程中使用时，完成回调会返回原 IO 线程，`wait()` 只会 Yield 当前协程。

## 环境要求

- Linux
- C++17 编译器
- CMake 3.8+
- Protobuf
- tinyxml
- C++ 标准线程库 / dl

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
bin/test_rpc_async_client
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

也可以运行独立异步客户端示例：

```bash
./bin/test_rpc_async_client 127.0.0.1 20000
```

该示例会验证 `CallMethod()` 立即返回、Closure 被执行以及 `wait()` 获得最终响应。

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
- `coroutine.coroutine_pool_size`：常驻协程数和栈内存单次扩容的 block 数，不是协程总数上限。
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
├── testcases/         # 示例服务端、同步/异步客户端、proto 和协程测试
└── LICENSE
```

## 当前边界

- 当前仅内置一套基于 Protobuf 的二进制 RPC 编解码实现。
- 当前网络模型依赖 Linux epoll、eventfd 等能力。
- 独立客户端异步调用当前复用进程内单个共享 IO 线程，通过多个协程并发执行 RPC。
- MySQL 相关代码是可选能力，需要通过 `CRPC_ENABLE_MYSQL` 构建选项启用。
- 项目暂不包含服务治理、注册中心、负载均衡等生产级功能。

## License

CRPC 使用 Apache License 2.0 开源协议，详情见 [LICENSE](LICENSE)。

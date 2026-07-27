/**
 * 日志模块
 * 提供多级别日志输出（DEBUG/INFO/WARN/ERROR），支持 RPC 框架日志和应用层日志
 * 日志通过异步线程写入文件，避免阻塞业务线程
 *
 * 使用方式:
 *   DebugLog << "message";           // RPC 框架日志（流式写法）
 *   AppInfoLog("format %s", arg);    // 应用层日志（printf 风格）
 */
#ifndef CRPC_BASE_LOG_H_
#define CRPC_BASE_LOG_H_

#include "crpc/base/config.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>
#include <sstream>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
#include <unistd.h>

namespace crpc {

extern Config::Ptr rpc_config;

// 格式化字符串工具函数，类似 sprintf 但返回 std::string
template <typename... Args>
std::string FormatString(const char* str, Args&&... args) {
	int size = snprintf(nullptr, 0, str, args...);

	std::string result;
	if (size > 0) {
		result.resize(size);
		snprintf(&result[0], size, str, args...);
	}

	return result;
}

// ============ RPC 框架日志宏 ============
// 使用流式写法: DebugLog << "xxx";
// 原理: 构造临时 LogTmp 对象，析构时自动将日志写入 Logger 缓冲区

#define DebugLog                                                                           \
	if (crpc::OpenLog() && crpc::LogLevel::kDebug >= crpc::rpc_config->log_level_)         \
	crpc::LogTmp(                                                                          \
		crpc::LogEvent::Ptr(new crpc::LogEvent(crpc::LogLevel::kDebug, __FILE__, __LINE__, \
											   __func__, crpc::LogType::kRpcLog)))         \
		.getStringStream()

#define InfoLog                                                                                    \
	if (crpc::OpenLog() && crpc::LogLevel::kInfo >= crpc::rpc_config->log_level_)                  \
	crpc::LogTmp(crpc::LogEvent::Ptr(new crpc::LogEvent(crpc::LogLevel::kInfo, __FILE__, __LINE__, \
														__func__, crpc::LogType::kRpcLog)))        \
		.getStringStream()

#define WarnLog                                                                                    \
	if (crpc::OpenLog() && crpc::LogLevel::kWarn >= crpc::rpc_config->log_level_)                  \
	crpc::LogTmp(crpc::LogEvent::Ptr(new crpc::LogEvent(crpc::LogLevel::kWarn, __FILE__, __LINE__, \
														__func__, crpc::LogType::kRpcLog)))        \
		.getStringStream()

#define ErrorLog                                                                           \
	if (crpc::OpenLog() && crpc::LogLevel::kError >= crpc::rpc_config->log_level_)         \
	crpc::LogTmp(                                                                          \
		crpc::LogEvent::Ptr(new crpc::LogEvent(crpc::LogLevel::kError, __FILE__, __LINE__, \
											   __func__, crpc::LogType::kRpcLog)))         \
		.getStringStream()

// ============ 应用层日志宏 ============
// 使用 printf 风格: AppInfoLog("user=%s count=%d", name, cnt);
// 写入独立的应用日志文件，与框架日志分离

#define AppDebugLog(str, ...)                                                            \
	if (crpc::OpenLog() && crpc::LogLevel::kDebug >= crpc::rpc_config->app_log_level_) { \
		crpc::Logger::getLogger()->pushAppLog(                                           \
			crpc::LogEvent(crpc::LogLevel::kDebug, __FILE__, __LINE__, __func__,         \
						   crpc::LogType::kAppLog)                                       \
				.toString() +                                                            \
			"[" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "]\t" +       \
			crpc::FormatString(str, ##__VA_ARGS__) + "\n");                              \
	}

#define AppInfoLog(str, ...)                                                            \
	if (crpc::OpenLog() && crpc::LogLevel::kInfo >= crpc::rpc_config->app_log_level_) { \
		crpc::Logger::getLogger()->pushAppLog(                                          \
			crpc::LogEvent(crpc::LogLevel::kInfo, __FILE__, __LINE__, __func__,         \
						   crpc::LogType::kAppLog)                                      \
				.toString() +                                                           \
			"[" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "]\t" +      \
			crpc::FormatString(str, ##__VA_ARGS__) + "\n");                             \
	}

#define AppWarnLog(str, ...)                                                            \
	if (crpc::OpenLog() && crpc::LogLevel::kWarn >= crpc::rpc_config->app_log_level_) { \
		crpc::Logger::getLogger()->pushAppLog(                                          \
			crpc::LogEvent(crpc::LogLevel::kWarn, __FILE__, __LINE__, __func__,         \
						   crpc::LogType::kAppLog)                                      \
				.toString() +                                                           \
			"[" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "]\t" +      \
			crpc::FormatString(str, ##__VA_ARGS__) + "\n");                             \
	}

#define AppErrorLog(str, ...)                                                            \
	if (crpc::OpenLog() && crpc::LogLevel::kError >= crpc::rpc_config->app_log_level_) { \
		crpc::Logger::getLogger()->pushAppLog(                                           \
			crpc::LogEvent(crpc::LogLevel::kError, __FILE__, __LINE__, __func__,         \
						   crpc::LogType::kAppLog)                                       \
				.toString() +                                                            \
			"[" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "]\t" +       \
			crpc::FormatString(str, ##__VA_ARGS__) + "\n");                              \
	}

// 日志类型：框架内部日志 vs 应用层日志，分别写入不同的日志文件
enum LogType {
	kRpcLog = 1,  // RPC 框架日志
	kAppLog = 2,  // 应用层日志
};

pid_t GetTid();

LogLevel StringToLevel(const std::string& str);
std::string LevelToString(LogLevel level);

bool OpenLog();

/**
 * 日志事件，封装一条日志的全部上下文信息（时间、级别、文件、行号、协程 ID等）
 * 生命周期: 构造 -> 通过 getStringStream() 写入内容 -> log() 提交到 Logger
 */
class LogEvent {
public:
	using Ptr = std::shared_ptr<LogEvent>;
	LogEvent(LogLevel level, const char* file_name, int line, const char* func_name, LogType type);

	~LogEvent();

	std::stringstream& getStringStream();

	// 将日志事件格式化为字符串（包含时间戳、级别、线程/协程 ID 等前缀）
	std::string toString();

	// 将此条日志提交到 Logger 的缓冲区
	void log();

private:
	timeval timeval_;  // 日志产生的时间戳
	LogLevel level_;   // 日志级别
	pid_t pid_{0};	   // 进程 ID
	pid_t tid_{0};	   // 线程 ID
	int cor_id_{0};	   // 协程 ID

	const char* file_name_;	 // 源文件名
	int line_{0};			 // 行号
	const char* func_name_;	 // 函数名
	LogType type_;			 // 日志类型（RPC/APP）
	std::string msg_no_;	 // 请求唯一标识号

	std::stringstream ss_;	// 日志内容流
};

/**
 * 日志临时对象，利用 RAII 机制实现流式日志写入
 * 构造时绑定 LogEvent，析构时自动调用 event->log() 提交日志
 * 配合宏使用: DebugLog << "msg" 实际上是 LogTmp(...).getStringStream() << "msg"
 */
class LogTmp {
public:
	explicit LogTmp(LogEvent::Ptr event);

	~LogTmp();

	std::stringstream& getStringStream();

private:
	LogEvent::Ptr event_;
};

/**
 * 异步日志写入器，在独立线程中将日志缓冲区的内容写入磁盘文件
 * 每个 AsyncLogger 管理一个日志文件，支持按日期和大小自动切分
 * 通过标准线程、互斥锁和条件变量实现生产者-消费者模型
 */
class AsyncLogger {
public:
	using Ptr = std::shared_ptr<AsyncLogger>;

	AsyncLogger(const char* file_name, const char* file_path, int max_size, LogType logtype);
	~AsyncLogger();

	// 将一批日志消息推入异步写入队列
	void push(std::vector<std::string>& buffer);

	// 将队列中的日志刷写到磁盘文件
	void flush();

	void stop();
	void join();

public:
	std::queue<std::vector<std::string>> tasks_;  // 待写入的日志批次队列

private:
	const char* file_name_;		  // 日志文件名前缀
	const char* file_path_;		  // 日志文件存放目录
	int max_size_{0};			  // 单个日志文件最大字节数
	LogType log_type_;			  // 日志类型
	int no_{0};					  // 当日日志文件序号（同一天超大小后递增）
	bool need_reopen_{false};	  // 是否需要重新打开文件（日期变化或文件过大）
	FILE* file_handle_{nullptr};  // 当前日志文件句柄
	std::string date_;			  // 当前日志文件对应的日期

	std::mutex mutex_;
	std::condition_variable condition_;	 // 通知异步线程有新日志或停止
	bool stop_{false};
	std::thread thread_;	 // 异步写入线程
	std::mutex file_mutex_;	 // 保护文件句柄及切分状态

	// 异步线程的入口函数
	void execute();
};

/**
 * 日志管理器（单例），协调 RPC 日志和应用日志的收集与分发
 * 内部维护两个缓冲区，定时将缓冲区内容交给对应的 AsyncLogger 异步写入磁盘
 */
class Logger {
public:
	static Logger* getLogger();

public:
	using Ptr = std::shared_ptr<Logger>;

	Logger();
	~Logger();

	// 初始化日志系统，设置文件名、路径、最大文件大小和同步间隔
	void init(const char* file_name, const char* file_path, int max_size, int sync_interval);

	void pushRpcLog(const std::string& log_msg);
	void pushAppLog(const std::string& log_msg);

	// 定时器回调：将缓冲区中积累的日志交给 AsyncLogger
	void loopFunc();

	void flush();

	void start();

	AsyncLogger::Ptr getAsyncLogger() { return async_rpc_logger_; }

	AsyncLogger::Ptr getAsyncAppLogger() { return async_app_logger_; }

public:
	std::vector<std::string> buffer_;	   // RPC 日志缓冲区
	std::vector<std::string> app_buffer_;  // 应用日志缓冲区

private:
	std::mutex app_buff_mutex_;	 // 应用日志缓冲区锁
	std::mutex buff_mutex_;		 // RPC 日志缓冲区锁
	bool is_init_{false};
	AsyncLogger::Ptr async_rpc_logger_;	 // RPC 日志异步写入器
	AsyncLogger::Ptr async_app_logger_;	 // 应用日志异步写入器

	int sync_interval_{0};	// 日志同步间隔（毫秒）
};

void Exit(int code);

}  // namespace crpc
#endif

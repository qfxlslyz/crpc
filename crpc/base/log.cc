#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <signal.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifdef DECLARE_MYSQL_PLUGIN
#include <mysql/mysql.h>
#endif

#include "crpc/base/config.h"
#include "crpc/base/log.h"
#include "crpc/base/run_time.h"
#include "crpc/coroutine/coroutine.h"
#include "crpc/net/event/reactor.h"
#include "crpc/net/event/timer.h"

namespace crpc {

extern Logger::Ptr rpc_logger;
extern Config::Ptr rpc_config;

static std::atomic_int64_t g_rpc_log_index{0};
static std::atomic_int64_t g_app_log_index{0};

void CoredumpHandler(int signal_no) {
	static const char message[] = "process received signal, will exit\n";
	syscall(SYS_write, STDERR_FILENO, message, sizeof(message) - 1);
	signal(signal_no, SIG_DFL);
	raise(signal_no);
}

class Coroutine;

static thread_local pid_t t_thread_id = 0;
static pid_t g_pid = 0;

// LogLevel g_log_level = kDebug;

pid_t GetTid() {
	if (t_thread_id == 0) {
		t_thread_id = syscall(SYS_gettid);
	}
	return t_thread_id;
}

void SetLogLevel(LogLevel level) {
	// g_log_level = level;
}

bool OpenLog() {
	if (!rpc_logger) {
		return false;
	}
	return true;
}

LogEvent::LogEvent(LogLevel level, const char* file_name, int line, const char* func_name,
				   LogType type)
	: level_(level), file_name_(file_name), line_(line), func_name_(func_name), type_(type) {}

LogEvent::~LogEvent() {}

std::string LevelToString(LogLevel level) {
	std::string re = "DEBUG";
	switch (level) {
		case kDebug:
			re = "DEBUG";
			return re;

		case kInfo:
			re = "INFO";
			return re;

		case kWarn:
			re = "WARN";
			return re;

		case kError:
			re = "ERROR";
			return re;
		case kNone:
			re = "NONE";

		default:
			return re;
	}
}

LogLevel StringToLevel(const std::string& str) {
	if (str == "DEBUG")
		return LogLevel::kDebug;

	if (str == "INFO")
		return LogLevel::kInfo;

	if (str == "WARN")
		return LogLevel::kWarn;

	if (str == "ERROR")
		return LogLevel::kError;

	if (str == "NONE")
		return LogLevel::kNone;

	return LogLevel::kDebug;
}

std::string LogTypeToString(LogType logtype) {
	switch (logtype) {
		case kAppLog:
			return "app";
		case kRpcLog:
			return "rpc";
		default:
			return "";
	}
}

std::stringstream& LogEvent::getStringStream() {
	// time_t now_time = timestamp_;

	gettimeofday(&timeval_, nullptr);

	struct tm time;
	localtime_r(&(timeval_.tv_sec), &time);

	const char* format = "%Y-%m-%d %H:%M:%S";
	char buf[128];
	strftime(buf, sizeof(buf), format, &time);

	ss_ << "[" << buf << "." << timeval_.tv_usec << "]\t";

	std::string s_level = LevelToString(level_);
	ss_ << "[" << s_level << "]\t";

	if (g_pid == 0) {
		g_pid = getpid();
	}
	pid_ = g_pid;

	if (t_thread_id == 0) {
		t_thread_id = GetTid();
	}
	tid_ = t_thread_id;

	cor_id_ = Coroutine::getCurrentCoroutine()->getCorId();

	ss_ << "[" << pid_ << "]\t"
		<< "[" << tid_ << "]\t"
		<< "[" << cor_id_ << "]\t"
		<< "[" << file_name_ << ":" << line_ << "]\t";
	// << "[" << func_name_ << "]\t";
	RunTime* runtime = GetCurrentRunTime();
	if (runtime) {
		std::string msgno = runtime->msg_no_;
		if (!msgno.empty()) {
			ss_ << "[" << msgno << "]\t";
		}

		std::string interface_name = runtime->interface_name_;
		if (!interface_name.empty()) {
			ss_ << "[" << interface_name << "]\t";
		}
	}
	return ss_;
}

std::string LogEvent::toString() {
	return getStringStream().str();
}

void LogEvent::log() {
	ss_ << "\n";
	if (level_ >= rpc_config->log_level_ && type_ == kRpcLog) {
		rpc_logger->pushRpcLog(ss_.str());
	} else if (level_ >= rpc_config->app_log_level_ && type_ == kAppLog) {
		rpc_logger->pushAppLog(ss_.str());
	}
}

LogTmp::LogTmp(LogEvent::Ptr event) : event_(event) {}

std::stringstream& LogTmp::getStringStream() {
	return event_->getStringStream();
}

LogTmp::~LogTmp() {
	event_->log();
}

Logger::Logger() {
	// 这里不能执行任何会调用 LOG 的操作，否则可能导致崩溃
}

Logger::~Logger() {
	flush();
}

Logger* Logger::getLogger() {
	return rpc_logger.get();
}

void Logger::init(const char* file_name, const char* file_path, int max_size, int sync_interval) {
	if (!is_init_) {
		sync_interval_ = sync_interval;
		for (int i = 0; i < 1000000; ++i) {
			app_buffer_.push_back("");
			buffer_.push_back("");
		}
		// app_buffer_.resize(1000000);
		// buffer_.resize(1000000);

		async_rpc_logger_ = std::make_shared<AsyncLogger>(file_name, file_path, max_size, kRpcLog);
		async_app_logger_ = std::make_shared<AsyncLogger>(file_name, file_path, max_size, kAppLog);

		signal(SIGSEGV, CoredumpHandler);
		signal(SIGABRT, CoredumpHandler);
		signal(SIGTERM, CoredumpHandler);
		signal(SIGINT, CoredumpHandler);
		signal(SIGSTKFLT, CoredumpHandler);

		// 忽略 SIGPIPE 信号
		signal(SIGPIPE, SIG_IGN);
		is_init_ = true;
	}
}

void Logger::start() {
	TimerEvent::Ptr event =
		std::make_shared<TimerEvent>(sync_interval_, true, std::bind(&Logger::loopFunc, this));
	Reactor::getReactor()->getTimer()->addTimerEvent(event);
}

void Logger::loopFunc() {
	std::vector<std::string> app_tmp;
	{
		std::lock_guard<std::mutex> lock(app_buff_mutex_);
		app_tmp.swap(app_buffer_);
	}

	std::vector<std::string> tmp;
	{
		std::lock_guard<std::mutex> lock(buff_mutex_);
		tmp.swap(buffer_);
	}

	async_rpc_logger_->push(tmp);
	async_app_logger_->push(app_tmp);
}

void Logger::pushRpcLog(const std::string& msg) {
	std::lock_guard<std::mutex> lock(buff_mutex_);
	buffer_.push_back(std::move(msg));
}

void Logger::pushAppLog(const std::string& msg) {
	std::lock_guard<std::mutex> lock(app_buff_mutex_);
	app_buffer_.push_back(std::move(msg));
}

void Logger::flush() {
	if (!async_rpc_logger_ || !async_app_logger_) {
		return;
	}
	loopFunc();
	async_rpc_logger_->stop();
	async_app_logger_->stop();
	async_rpc_logger_->join();
	async_app_logger_->join();
}

AsyncLogger::AsyncLogger(const char* file_name, const char* file_path, int max_size,
						 LogType logtype)
	: file_name_(file_name),
	  file_path_(file_path),
	  max_size_(max_size),
	  log_type_(logtype),
	  thread_(&AsyncLogger::execute, this) {}

AsyncLogger::~AsyncLogger() {
	stop();
	join();
}

void AsyncLogger::execute() {
	while (true) {
		std::vector<std::string> tmp;
		{
			std::unique_lock<std::mutex> lock(mutex_);
			condition_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });
			if (tasks_.empty()) {
				break;
			}
			tmp = std::move(tasks_.front());
			tasks_.pop();
		}

		timeval now;
		gettimeofday(&now, nullptr);

		struct tm now_time;
		localtime_r(&(now.tv_sec), &now_time);

		const char* format = "%Y%m%d";
		char date[32];
		strftime(date, sizeof(date), format, &now_time);
		std::lock_guard<std::mutex> file_lock(file_mutex_);
		if (date_ != std::string(date)) {
			// 跨天
			// 重置 no_ 和 date_
			no_ = 0;
			date_ = std::string(date);
			need_reopen_ = true;
		}

		if (!file_handle_) {
			need_reopen_ = true;
		}

		std::stringstream ss;
		ss << file_path_ << file_name_ << "_" << date_ << "_" << LogTypeToString(log_type_) << "_"
		   << no_ << ".log";
		std::string full_file_name = ss.str();

		if (need_reopen_) {
			if (file_handle_) {
				fclose(file_handle_);
			}

			file_handle_ = fopen(full_file_name.c_str(), "a");
			if (file_handle_ == nullptr) {
				printf("open fail errno = %d reason = %s \n", errno, strerror(errno));
			}
			need_reopen_ = false;
		}

		if (file_handle_ && ftell(file_handle_) > max_size_) {
			fclose(file_handle_);
			file_handle_ = nullptr;

			// 单个日志文件超过最大大小
			no_++;
			std::stringstream ss2;
			ss2 << file_path_ << file_name_ << "_" << date_ << "_" << LogTypeToString(log_type_)
				<< "_" << no_ << ".log";
			full_file_name = ss2.str();

			file_handle_ = fopen(full_file_name.c_str(), "a");
			need_reopen_ = false;
		}

		if (!file_handle_) {
			printf("open log file %s error!", full_file_name.c_str());
			continue;
		}

		for (const auto& i : tmp) {
			if (!i.empty()) {
				fwrite(i.c_str(), 1, i.length(), file_handle_);
			}
		}
		fflush(file_handle_);
	}

	std::lock_guard<std::mutex> file_lock(file_mutex_);
	if (file_handle_) {
		fflush(file_handle_);
		fclose(file_handle_);
		file_handle_ = nullptr;
	}
}

void AsyncLogger::push(std::vector<std::string>& buffer) {
	if (buffer.empty()) {
		return;
	}
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (stop_) {
			return;
		}
		tasks_.push(buffer);
	}
	condition_.notify_one();
}

void AsyncLogger::flush() {
	std::lock_guard<std::mutex> lock(file_mutex_);
	if (file_handle_) {
		fflush(file_handle_);
	}
}

void AsyncLogger::stop() {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (stop_) {
			return;
		}
		stop_ = true;
	}
	condition_.notify_all();
}

void AsyncLogger::join() {
	if (thread_.joinable()) {
		thread_.join();
	}
}

void Exit(int code) {
#ifdef DECLARE_MYSQL_PLUGIN
	mysql_library_end();
#endif

	printf(
		"It's sorry to said we Start CRPC server error, look up log file to "
		"get "
		"more details!\n");
	if (rpc_logger) {
		rpc_logger->flush();
	}

	_exit(code);
}

}  // namespace crpc

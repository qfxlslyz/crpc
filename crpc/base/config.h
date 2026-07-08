/**
 * 配置模块
 * 从 XML
 * 配置文件中读取服务器运行所需的全部参数，包括日志、协程、网络、数据库等配置
 * 使用 TinyXML 库解析 XML 文件
 */
#ifndef CRPC_BASE_CONFIG_H_
#define CRPC_BASE_CONFIG_H_

#include <map>
#include <memory>
#include <string>

#include <tinyxml/tinyxml.h>

#ifdef DECLARE_MYSQL_PLUGIN
#include "crpc/plugins/mysql/mysql_instance.h"
#endif

namespace crpc {

// 日志级别枚举，数值越大级别越高，可通过配置文件控制输出级别
enum LogLevel {
	kDebug = 1,
	kInfo = 2,
	kWarn = 3,
	kError = 4,
	kNone = 5  // 不输出任何日志
};

/**
 * 全局配置类，解析 XML 配置文件并保存所有运行参数
 * 在服务器启动时由 InitConfig() 创建，全局唯一实例通过 rpc_config 访问
 */
class Config {
public:
	using Ptr = std::shared_ptr<Config>;

	Config(const char* file_path);

	~Config();

	// 读取全部配置项（日志、协程、网络、时间轮等）
	void readConf();

	void readDBConfig(TiXmlElement* node);

	void readLogConfig(TiXmlElement* node);

	// 获取指定名称的 XML 子节点
	TiXmlElement* getXmlNode(const std::string& name);

public:
	// ---- 日志相关配置 ----
	std::string log_path_;						// 日志文件存放路径
	std::string log_prefix_;					// 日志文件名前缀
	int log_max_size_{0};						// 单个日志文件最大大小（字节）
	LogLevel log_level_{LogLevel::kDebug};		// RPC 框架日志级别
	LogLevel app_log_level_{LogLevel::kDebug};	// 应用层日志级别
	int log_sync_interval_{500};				// 日志同步间隔（毫秒）

	// ---- 协程相关配置 ----
	int cor_stack_size_{0};	 // 每个协程的栈大小（字节）
	int cor_pool_size_{0};	 // 协程池中协程数量

	int msg_req_len_{0};  // 请求 ID 长度

	int max_connect_timeout_{0};  // 连接超时时间（毫秒）
	int iothread_num_{0};		  // IO 线程数量

	// ---- 时间轮相关配置（用于连接超时检测）----
	int timewheel_bucket_num_{0};  // 时间轮桶数量
	int timewheel_interval_{0};	   // 时间轮转动间隔（秒）

	// ---- 服务监听配置 ----
	std::string server_ip_;		   // 服务监听 IP
	int server_port_{0};		   // 服务监听端口
	std::string server_protocol_;  // RPC 协议类型

#ifdef DECLARE_MYSQL_PLUGIN
	std::map<std::string, MySQLOption> mysql_options_;	// MySQL 数据库配置
#endif

private:
	std::string file_path_;	 // 配置文件路径

	TiXmlDocument* xml_file_;  // XML 文档对象
};

}  // namespace crpc
#endif	// CRPC_BASE_CONFIG_H

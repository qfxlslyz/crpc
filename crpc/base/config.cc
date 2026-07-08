#include "crpc/base/config.h"
#include "crpc/base/log.h"

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stdio.h>

#include <tinyxml/tinyxml.h>

namespace crpc {

extern Logger::Ptr rpc_logger;	// 全局日志管理器

// 构造函数: 加载并解析 XML 配置文件，解析失败则直接退出进程
Config::Config(const char* file_path) : file_path_(std::string(file_path)) {
	xml_file_ = new TiXmlDocument();
	bool rt = xml_file_->LoadFile(file_path);
	if (!rt) {
		printf(
			"Start crpc server error! read conf file [%s] error info: [%s], "
			"errorid: [%d], error_row_column:[%d row %d column]\n",
			file_path, xml_file_->ErrorDesc(), xml_file_->ErrorId(), xml_file_->ErrorRow(),
			xml_file_->ErrorCol());
		exit(0);
	}
}

void Config::readLogConfig(TiXmlElement* log_node) {
	TiXmlElement* node = log_node->FirstChildElement("log_path");
	if (!node || !node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[log_path] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	log_path_ = std::string(node->GetText());

	node = log_node->FirstChildElement("log_prefix");
	if (!node || !node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[log_prefix] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	log_prefix_ = std::string(node->GetText());

	node = log_node->FirstChildElement("log_max_file_size");
	if (!node || !node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[log_max_file_size] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	int log_max_size = std::atoi(node->GetText());
	log_max_size_ = log_max_size * 1024 * 1024;

	node = log_node->FirstChildElement("rpc_log_level");
	if (!node || !node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[rpc_log_level] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	std::string log_level = std::string(node->GetText());
	log_level_ = StringToLevel(log_level);

	node = log_node->FirstChildElement("app_log_level");
	if (!node || !node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[app_log_level] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	log_level = std::string(node->GetText());
	app_log_level_ = StringToLevel(log_level);

	node = log_node->FirstChildElement("log_sync_interval");
	if (!node) {
		node = log_node->FirstChildElement("log_sync_inteval");
	}
	if (!node || !node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[log_sync_interval] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	log_sync_interval_ = std::atoi(node->GetText());

	rpc_logger = std::make_shared<Logger>();
	rpc_logger->init(log_prefix_.c_str(), log_path_.c_str(), log_max_size_, log_sync_interval_);
}

void Config::readDBConfig(TiXmlElement* node) {
#ifdef DECLARE_MYSQL_PLUGIN

	printf("read db config\n");
	if (!node) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[database] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	for (TiXmlElement* element = node->FirstChildElement("db_key"); element != nullptr;
		 element = element->NextSiblingElement()) {
		std::string key = element->FirstAttribute()->Value();
		printf("key is %s\n", key.c_str());
		TiXmlElement* ip_e = element->FirstChildElement("ip");
		std::string ip;
		int port = 3306;
		if (ip_e) {
			ip = std::string(ip_e->GetText());
		}
		if (ip.empty()) {
			continue;
		}

		TiXmlElement* port_e = element->FirstChildElement("port");
		if (port_e && port_e->GetText()) {
			port = std::atoi(port_e->GetText());
		}

		MySQLOption option(IPAddress(ip, port));

		TiXmlElement* user_e = element->FirstChildElement("user");
		if (user_e && user_e->GetText()) {
			option.user_ = std::string(user_e->GetText());
		}

		TiXmlElement* passwd_e = element->FirstChildElement("passwd");
		if (passwd_e && passwd_e->GetText()) {
			option.passwd_ = std::string(passwd_e->GetText());
		}

		TiXmlElement* select_db_e = element->FirstChildElement("select_db");
		if (select_db_e && select_db_e->GetText()) {
			option.select_db_ = std::string(select_db_e->GetText());
		}

		TiXmlElement* char_set_e = element->FirstChildElement("char_set");
		if (char_set_e && char_set_e->GetText()) {
			option.char_set_ = std::string(char_set_e->GetText());
		}
		mysql_options_.insert(std::make_pair(key, option));
		char buf[512];
		sprintf(buf,
				"read config from file [%s], key:%s {addr: %s, user: %s, passwd: "
				"%s, select_db: %s, charset: %s}\n",
				file_path_.c_str(), key.c_str(), option.addr_.toString().c_str(),
				option.user_.c_str(), option.passwd_.c_str(), option.select_db_.c_str(),
				option.char_set_.c_str());
		std::string s(buf);
		InfoLog << s;
	}

#endif
}

// 读取全部配置：日志 -> 时间轮 -> 协程 -> 网络 -> 数据库
void Config::readConf() {
	TiXmlElement* root = xml_file_->RootElement();
	TiXmlElement* log_node = root->FirstChildElement("log");
	if (!log_node) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[log] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	readLogConfig(log_node);

	TiXmlElement* time_wheel_node = root->FirstChildElement("time_wheel");
	if (!time_wheel_node) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[time_wheel] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	TiXmlElement* coroutine_node = root->FirstChildElement("coroutine");
	if (!coroutine_node) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[coroutine] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	if (!coroutine_node->FirstChildElement("coroutine_stack_size") ||
		!coroutine_node->FirstChildElement("coroutine_stack_size")->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[coroutine.coroutine_stack_size] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	if (!coroutine_node->FirstChildElement("coroutine_pool_size") ||
		!coroutine_node->FirstChildElement("coroutine_pool_size")->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[coroutine.coroutine_pool_size] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	int cor_stack_size =
		std::atoi(coroutine_node->FirstChildElement("coroutine_stack_size")->GetText());
	cor_stack_size_ = 1024 * cor_stack_size;  // 配置文件中单位为 KB，转换为字节
	cor_pool_size_ = std::atoi(coroutine_node->FirstChildElement("coroutine_pool_size")->GetText());

	if (!root->FirstChildElement("msg_req_len") ||
		!root->FirstChildElement("msg_req_len")->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[msg_req_len] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	msg_req_len_ = std::atoi(root->FirstChildElement("msg_req_len")->GetText());

	if (!root->FirstChildElement("max_connect_timeout") ||
		!root->FirstChildElement("max_connect_timeout")->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[max_connect_timeout] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	int max_connect_timeout = std::atoi(root->FirstChildElement("max_connect_timeout")->GetText());
	max_connect_timeout_ = max_connect_timeout * 1000;	// 配置文件中单位为秒，转换为毫秒

	if (!root->FirstChildElement("iothread_num") ||
		!root->FirstChildElement("iothread_num")->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[iothread_num] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	iothread_num_ = std::atoi(root->FirstChildElement("iothread_num")->GetText());

	if (!time_wheel_node->FirstChildElement("bucket_num") ||
		!time_wheel_node->FirstChildElement("bucket_num")->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[time_wheel.bucket_num] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	TiXmlElement* timewheel_interval_node = time_wheel_node->FirstChildElement("interval");
	if (!timewheel_interval_node) {
		timewheel_interval_node = time_wheel_node->FirstChildElement("inteval");
	}
	if (!timewheel_interval_node || !timewheel_interval_node->GetText()) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[time_wheel.interval] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	timewheel_bucket_num_ = std::atoi(time_wheel_node->FirstChildElement("bucket_num")->GetText());
	timewheel_interval_ = std::atoi(timewheel_interval_node->GetText());

	TiXmlElement* net_node = root->FirstChildElement("server");
	if (!net_node) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[server] xml node\n",
			file_path_.c_str());
		exit(0);
	}

	TiXmlElement* protocol_node = net_node->FirstChildElement("protocol");
	if (!protocol_node) {
		protocol_node = net_node->FirstChildElement("protocal");
	}
	if (!net_node->FirstChildElement("ip") || !net_node->FirstChildElement("port") ||
		!protocol_node) {
		printf(
			"Start crpc server error! read config file [%s] error, cannot read "
			"[server.ip] or [server.port] or [server.protocol] xml node\n",
			file_path_.c_str());
		exit(0);
	}
	std::string ip = std::string(net_node->FirstChildElement("ip")->GetText());
	if (ip.empty()) {
		ip = "0.0.0.0";
	}
	int port = std::atoi(net_node->FirstChildElement("port")->GetText());
	if (port == 0) {
		printf(
			"Start crpc server error! read config file [%s] error, read "
			"[server.port] = 0\n",
			file_path_.c_str());
		exit(0);
	}
	std::string protocol = std::string(protocol_node->GetText());
	std::transform(protocol.begin(), protocol.end(), protocol.begin(), toupper);

	if (protocol == "RPC" || protocol == "DEFAULT") {
		protocol = "CRPC";
	}
	if (protocol != "CRPC") {
		printf(
			"Start crpc server error! only CRPC default rpc protocol is "
			"supported, "
			"got [%s]\n",
			protocol.c_str());
		exit(0);
	}
	server_ip_ = ip;
	server_port_ = port;
	server_protocol_ = protocol;

	char buff[512];
	sprintf(buff,
			"read config from file [%s]: [log_path: %s], [log_prefix: %s], "
			"[log_max_size: %d MB], [log_level: %s], "
			"[coroutine_stack_size: %d KB], [coroutine_pool_size: %d], "
			"[msg_req_len: %d], [max_connect_timeout: %d s], "
			"[iothread_num:%d], [timewheel_bucket_num: %d], [timewheel_interval: "
			"%d s], [server_ip: %s], [server_port: %d], [server_protocol: %s]\n",
			file_path_.c_str(), log_path_.c_str(), log_prefix_.c_str(), log_max_size_ / 1024 / 1024,
			LevelToString(log_level_).c_str(), cor_stack_size, cor_pool_size_, msg_req_len_,
			max_connect_timeout, iothread_num_, timewheel_bucket_num_, timewheel_interval_,
			ip.c_str(), port, protocol.c_str());

	std::string s(buff);
	InfoLog << s;

	TiXmlElement* database_node = root->FirstChildElement("database");

	if (database_node) {
		readDBConfig(database_node);
	}
}

Config::~Config() {
	if (xml_file_) {
		delete xml_file_;
		xml_file_ = nullptr;
	}
}

TiXmlElement* Config::getXmlNode(const std::string& name) {
	return xml_file_->RootElement()->FirstChildElement(name.c_str());
}

}  // namespace crpc

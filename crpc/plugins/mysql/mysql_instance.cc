#ifdef DECLARE_MYSQL_PLUGIN
#include <mysql/errmsg.h>
#include <mysql/mysql.h>
#endif

#include "crpc/base/config.h"
#include "crpc/base/log.h"
#include "crpc/coroutine/coroutine_hook.h"
#include "crpc/plugins/mysql/mysql_instance.h"

extern crpc::Config::Ptr rpc_config;

namespace crpc {

#ifdef DECLARE_MYSQL_PLUGIN

static thread_local MySQLInstanceFactory* t_mysql_factory = nullptr;

MySQLThreadInit::MySQLThreadInit() {
	DebugLog << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
				"<<<<<<<<<<<< call mysql_thread_init";
	mysql_thread_init();
}

MySQLThreadInit::~MySQLThreadInit() {
	mysql_thread_end();
	DebugLog << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
				">>>>>>>>>>>>>> call mysql_thread_end";
}

MySQLInstanceFactory* MySQLInstanceFactory::getThreadMySQLFactory() {
	if (t_mysql_factory) {
		return t_mysql_factory;
	}
	t_mysql_factory = new MySQLInstanceFactory();
	return t_mysql_factory;
}

MySQLInstance::Ptr MySQLInstanceFactory::getMySQLInstance(const std::string& key) {
	auto it2 = rpc_config->mysql_options_.find(key);
	if (it2 == rpc_config->mysql_options_.end()) {
		ErrorLog << "get MySQLInstance error, not this key[" << key << "] exist";
		return nullptr;
	}
	DebugLog << "create MySQLInstance of key " << key;
	MySQLInstance::Ptr instance = std::make_shared<MySQLInstance>(it2->second);
	return instance;
}

MySQLInstance::MySQLInstance(const MySQLOption& option) : option_(option) {
	int ret = reconnect();
	if (ret != 0) {
		return;
	}

	init_succ_ = true;
}

int MySQLInstance::reconnect() {
	// 这个静态对象只会初始化一次
	// 第一次调用 MySQLInstance::reconnect 时会执行 mysql_thread_init
	// 当前线程销毁时会执行 mysql_thread_end
	static thread_local MySQLThreadInit t_mysql_thread_init;

	if (sql_handler_) {
		mysql_close(sql_handler_);
		sql_handler_ = nullptr;
	}

	Mutex::ScopedLock lock(mutex_);
	sql_handler_ = mysql_init(nullptr);
	lock.unlock();
	if (!sql_handler_) {
		ErrorLog << "failed to call mysql_init allocate MYSQL instance";
		return -1;
	}
	// int value = 0;
	// mysql_options(sql_handler_, MYSQL_OPT_RECONNECT, &value);
	if (!option_.char_set_.empty()) {
		mysql_options(sql_handler_, MYSQL_SET_CHARSET_NAME, option_.char_set_.c_str());
	}
	DebugLog << "begin to connect mysql{ip:" << option_.addr_.getIP()
			 << ", port:" << option_.addr_.getPort() << ", user:" << option_.user_
			 << ", passwd:" << option_.passwd_ << ", select_db: " << option_.select_db_
			 << "charset:" << option_.char_set_ << "}";
	// mysql_real_connect(sql_handler_, option_.addr_.getIP().c_str(),
	// option_.user_.c_str(),
	//     option_.passwd_.c_str(), option_.select_db_.c_str(),
	//     option_.addr_.getPort(), nullptr, 0);
	if (!mysql_real_connect(sql_handler_, option_.addr_.getIP().c_str(), option_.user_.c_str(),
							option_.passwd_.c_str(), option_.select_db_.c_str(),
							option_.addr_.getPort(), nullptr, 0)) {
		ErrorLog << "failed to call mysql_real_connect, peer addr[ " << option_.addr_.getIP() << ":"
				 << option_.addr_.getPort() << "], mysql sys errinfo[" << mysql_error(sql_handler_)
				 << "]";
		return -1;
	}
	DebugLog << "mysql_handler connect succ";
	return 0;
}

bool MySQLInstance::isInitSuccess() {
	return init_succ_;
}

MySQLInstance::~MySQLInstance() {
	if (sql_handler_) {
		mysql_close(sql_handler_);
		sql_handler_ = nullptr;
	}
}

int MySQLInstance::commit() {
	int rt = query("COMMIT;");
	if (rt == 0) {
		in_trans_ = false;
	}
	return rt;
}

int MySQLInstance::begin() {
	int rt = query("BEGIN;");
	if (rt == 0) {
		in_trans_ = true;
	}
	return rt;
}

int MySQLInstance::rollBack() {
	int rt = query("ROLLBACK;");
	if (rt == 0) {
		in_trans_ = false;
	}
	return rt;
}

int MySQLInstance::query(const std::string& sql) {
	if (!init_succ_) {
		ErrorLog << "query error, mysql_handler init failed";
		return -1;
	}
	if (!sql_handler_) {
		DebugLog << "*************** will reconnect mysql ";
		reconnect();
	}
	if (!sql_handler_) {
		DebugLog << "reconnect error, query return -1";
		return -1;
	}

	DebugLog << "begin to execute sql[" << sql << "]";
	int rt = mysql_real_query(sql_handler_, sql.c_str(), sql.length());
	if (rt != 0) {
		ErrorLog << "execute mysql_real_query error, sql[" << sql << "], mysql sys errinfo["
				 << mysql_error(sql_handler_) << "]";
		// 如果连接出错，开始重连
		if (mysql_errno(sql_handler_) == CR_SERVER_GONE_ERROR ||
			mysql_errno(sql_handler_) == CR_SERVER_LOST) {
			rt = reconnect();
			if (rt != 0 && !in_trans_) {
				// 如果重连成功且当前不在事务中，可以重新执行 SQL 查询
				rt = mysql_real_query(sql_handler_, sql.c_str(), sql.length());
				return rt;
			}
		}
	} else {
		InfoLog << "execute mysql_real_query success, sql[" << sql << "]";
	}
	return rt;
}

MYSQL_RES* MySQLInstance::storeResult() {
	if (!init_succ_) {
		ErrorLog << "query error, mysql_handler init failed";
		return nullptr;
	}
	int count = mysql_field_count(sql_handler_);
	if (count != 0) {
		MYSQL_RES* res = mysql_store_result(sql_handler_);
		if (!res) {
			ErrorLog << "execute mysql_store_result error, mysql sys errinfo["
					 << mysql_error(sql_handler_) << "]";
		} else {
			DebugLog << "execute mysql_store_result success";
		}
		return res;
	} else {
		DebugLog << "mysql_field_count = 0, not need store result";
		return nullptr;
	}
}

MYSQL_ROW MySQLInstance::fetchRow(MYSQL_RES* res) {
	if (!init_succ_) {
		ErrorLog << "query error, mysql_handler init failed";
		return nullptr;
	}
	return mysql_fetch_row(res);
}

long long MySQLInstance::numFields(MYSQL_RES* res) {
	if (!init_succ_) {
		ErrorLog << "query error, mysql_handler init failed";
		return -1;
	}
	return mysql_num_fields(res);
}

void MySQLInstance::freeResult(MYSQL_RES* res) {
	if (!init_succ_) {
		ErrorLog << "query error, mysql_handler init failed";
		return;
	}
	if (!res) {
		DebugLog << "free result error, res is null";
		return;
	}
	mysql_free_result(res);
}

long long MySQLInstance::affectedRows() {
	if (!init_succ_) {
		ErrorLog << "query error, mysql_handler init failed";
		return -1;
	}
	return mysql_affected_rows(sql_handler_);
}

std::string MySQLInstance::getMySQLErrorInfo() {
	return std::string(mysql_error(sql_handler_));
}

int MySQLInstance::getMySQLErrno() {
	return mysql_errno(sql_handler_);
}

#endif

}  // namespace crpc

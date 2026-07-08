#ifndef CRPC_PLUGINS_MYSQL_MYSQL_INSTANCE_H_
#define CRPC_PLUGINS_MYSQL_MYSQL_INSTANCE_H_

#ifdef DECLARE_MYSQL_PLUGIN
#include <mysql/mysql.h>
#endif

#include "crpc/base/mutex.h"
#include "crpc/net/transport/net_address.h"

#include <map>
#include <memory>

namespace crpc {

struct MySQLOption {
public:
	explicit MySQLOption(const IPAddress& addr) : addr_(addr){};
	~MySQLOption(){};

public:
	IPAddress addr_;
	std::string user_;
	std::string passwd_;
	std::string select_db_;
	std::string char_set_;
};

#ifdef DECLARE_MYSQL_PLUGIN
class MySQLThreadInit {
public:
	MySQLThreadInit();

	~MySQLThreadInit();
};

class MySQLInstance {
public:
	using Ptr = std::shared_ptr<MySQLInstance>;

	MySQLInstance(const MySQLOption& option);

	~MySQLInstance();

	bool isInitSuccess();

	int query(const std::string& sql);

	int commit();

	int begin();

	int rollBack();

	MYSQL_RES* storeResult();

	MYSQL_ROW fetchRow(MYSQL_RES* res);

	void freeResult(MYSQL_RES* res);

	long long numFields(MYSQL_RES* res);

	long long affectedRows();

	std::string getMySQLErrorInfo();

	int getMySQLErrno();

private:
	int reconnect();

private:
	MySQLOption option_;
	bool init_succ_{false};
	bool in_trans_{false};
	Mutex mutex_;
	MYSQL* sql_handler_{nullptr};
};

class MySQLInstanceFactory {
public:
	MySQLInstanceFactory() = default;

	~MySQLInstanceFactory() = default;

	MySQLInstance::Ptr getMySQLInstance(const std::string& key);

public:
	static MySQLInstanceFactory* getThreadMySQLFactory();
};

#endif

}  // namespace crpc

#endif

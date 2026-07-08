#include "crpc/base/config.h"
#include "crpc/base/log.h"
#include "crpc/base/msg_req.h"

#include <fcntl.h>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace crpc {

extern Config::Ptr rpc_config;

static thread_local std::string t_msg_req_nu;
static thread_local std::string t_max_msg_req_nu;

static int g_random_fd = -1;

std::string MsgReqUtil::genMsgNumber() {
	int msg_req_len = 20;  // RPC请求ID的长度，单位 bit
	if (rpc_config) {
		msg_req_len = rpc_config->msg_req_len_;
	}

	if (t_msg_req_nu.empty() || t_msg_req_nu == t_max_msg_req_nu) {
		if (g_random_fd == -1) {
			g_random_fd = open("/dev/urandom", O_RDONLY);
		}
		std::string res(msg_req_len, 0);
		if ((read(g_random_fd, &res[0], msg_req_len)) != msg_req_len) {
			ErrorLog << "read /dev/urandom data less " << msg_req_len << " bytes";
			return "";
		}
		t_max_msg_req_nu = "";
		for (int i = 0; i < msg_req_len; ++i) {
			uint8_t x = ((uint8_t)(res[i])) % 10;
			res[i] = x + '0';
			t_max_msg_req_nu += "9";
		}
		t_msg_req_nu = res;

	} else {
		int i = t_msg_req_nu.length() - 1;
		while (i >= 0 && t_msg_req_nu[i] == '9') {
			i--;
		}
		if (i >= 0) {
			t_msg_req_nu[i] += 1;
			for (size_t j = i + 1; j < t_msg_req_nu.length(); ++j) {
				t_msg_req_nu[j] = '0';
			}
		}
	}
	return t_msg_req_nu;
}

}  // namespace crpc

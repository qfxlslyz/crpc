/**
 * 字符串工具类
 * 提供通用字符串分割功能
 */
#ifndef CRPC_BASE_STRING_UTIL_H_
#define CRPC_BASE_STRING_UTIL_H_

#include <map>
#include <string>
#include <vector>

namespace crpc {

class StringUtil {
public:
	// 将字符串按分隔符拆分为 key-value 键值对
	// 例: str="a=1&tt=2&cc=3", split_str="&", joiner="="
	//     => res = {"a":"1", "tt":"2", "cc":"3"}
	static void splitStrToMap(const std::string& str, const std::string& split_str,
							  const std::string& joiner, std::map<std::string, std::string>& res);

	// 将字符串按分隔符拆分为字符串数组
	// 例: str="a=1&tt=2&cc=3", split_str="&"
	//     => res = {"a=1", "tt=2", "cc=3"}
	static void splitStrToVector(const std::string& str, const std::string& split_str,
								 std::vector<std::string>& res);
};

}  // namespace crpc

#endif

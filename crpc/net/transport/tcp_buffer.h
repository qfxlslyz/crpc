#ifndef CRPC_NET_TRANSPORT_TCP_BUFFER_H_
#define CRPC_NET_TRANSPORT_TCP_BUFFER_H_

#include <memory>
#include <vector>

namespace crpc {

/// TcpBuffer
/// +-------------------+------------------+------------------+
/// |     已消费字节     |      可读字节     |      可写字节     |
/// |                   |       内容        |                  |
/// +-------------------+------------------+------------------+
/// |                   |                  |                  |
/// 0      <=        读下标      <=      写下标       <=     容量
class TcpBuffer {
public:
	using Ptr = std::shared_ptr<TcpBuffer>;

	explicit TcpBuffer(int size);

	~TcpBuffer();

	// 当前可读字节数：write_index_ - read_index_
	int readAble();

	// 当前可写字节数：size_ - write_index_
	int writeAble();

	int readIndex() const;

	int writeIndex() const;

	// int readFormSocket(char* buf, int size);

	// 向缓冲区尾部追加数据，不足时会触发扩容
	void writeToBuffer(const char* buf, int size);

	// 从缓冲区读取 size 字节到 re，并推进读下标
	void readFromBuffer(std::vector<char>& re, int size);

	// 扩容或重建缓冲区，保留尚未消费的数据
	void resizeBuffer(int size);

	void clearBuffer();

	int getSize();

	// const char* getBuffer();

	std::vector<char> getBufferVector();

	std::string getBufferString();

	// 消费 index 字节数据
	void recycleRead(int index);

	// 写入 index 字节数据
	void recycleWrite(int index);

	// 当已消费区域较大时移动剩余数据，释放前部空间
	void adjustBuffer();

private:
	int read_index_{0};
	int write_index_{0};
	int size_{0};

public:
	std::vector<char> buffer_;
};

}  // namespace crpc

#endif

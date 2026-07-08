/**
 * 协程上下文（寄存器状态）
 * 定义协程切换时需要保存和恢复的 CPU 寄存器
 * coctx_swap
 * 是用汇编实现的核心切换函数，负责保存当前寄存器状态并恢复目标协程的寄存器
 */
#ifndef CRPC_COROUTINE_COCTX_H_
#define CRPC_COROUTINE_COCTX_H_

namespace crpc {

// x86-64 架构下关键寄存器在 regs 数组中的索引
enum {
	kRBP = 6,	   // rbp - 栈底指针
	kRDI = 7,	   // rdi - 函数调用的第一个参数
	kRSI = 8,	   // rsi - 函数调用的第二个参数
	kRETAddr = 9,  // 返回地址，切换后赋值给 rip（程序计数器）
	kRSP = 13,	   // rsp - 栈顶指针
};

// 协程上下文结构体，保存 14 个通用寄存器的值
struct coctx {
	void* regs[14];
};

extern "C" {
// 协程切换核心函数（汇编实现）：
// 将当前 CPU 寄存器状态保存到第一个 coctx，再从第二个 coctx 恢复寄存器状态
extern void coctx_swap(coctx*, coctx*) asm("coctx_swap");
};

}  // namespace crpc
#endif

#include "crpc/base/log.h"
#include "crpc/base/run_time.h"
#include "crpc/coroutine/coroutine.h"

#include <assert.h>
#include <atomic>
#include <stdlib.h>
#include <string.h>

namespace crpc {

// 主协程，每个 IO 线程都有一个 main_coroutine
static thread_local Coroutine* t_main_coroutine = nullptr;

// 当前线程正在运行的协程
static thread_local Coroutine* t_cur_coroutine = nullptr;

static thread_local RunTime* t_cur_run_time = nullptr;

// static thread_local bool t_enable_coroutine_swap = true;

static std::atomic_int t_coroutine_count{0};

static std::atomic_int t_cur_coroutine_id{1};

int GetCoroutineIndex() {
	return t_cur_coroutine_id;
}

RunTime* GetCurrentRunTime() {
	return t_cur_run_time;
}

void SetCurrentRunTime(RunTime* v) {
	t_cur_run_time = v;
}

void CoFunction(Coroutine* co) {
	if (co != nullptr) {
		co->setIsInCoFunc(true);

		// 去执行协程回调函数
		co->call_back_();

		co->setIsInCoFunc(false);
	}

	// 协程回调函数已执行完毕，说明该协程生命周期结束，需要切回主协程
	Coroutine::Yield();
}

// void Coroutine::SetCoroutineSwapFlag(bool value) {
//   t_enable_coroutine_swap = value;
// }

// bool Coroutine::GetCoroutineSwapFlag() {
//   return t_enable_coroutine_swap;
// }

Coroutine::Coroutine() {
	// 主协程 ID 为 0
	cor_id_ = 0;
	t_coroutine_count++;
	memset(&coctx_, 0, sizeof(coctx_));
	t_cur_coroutine = this;
}

Coroutine::Coroutine(int size, char* stack_ptr) : stack_size_(size), stack_sp_(stack_ptr) {
	assert(stack_ptr);

	if (!t_main_coroutine) {
		t_main_coroutine = new Coroutine();
	}

	cor_id_ = t_cur_coroutine_id++;
	t_coroutine_count++;
}

Coroutine::Coroutine(int size, char* stack_ptr, std::function<void()> cb)
	: stack_size_(size), stack_sp_(stack_ptr) {
	assert(stack_sp_);

	if (!t_main_coroutine) {
		t_main_coroutine = new Coroutine();
	}

	setCallBack(cb);
	cor_id_ = t_cur_coroutine_id++;
	t_coroutine_count++;
}

bool Coroutine::setCallBack(std::function<void()> cb) {
	if (this == t_main_coroutine) {
		ErrorLog << "main coroutine can't set callback";
		return false;
	}
	if (is_in_cofunc_) {
		ErrorLog << "this coroutine is in CoFunction";
		return false;
	}

	call_back_ = cb;

	// assert(stack_sp_ != nullptr);

	char* top = stack_sp_ + stack_size_;
	// 先将栈清零
	// memset(&top, 0, stack_size_);

	top = reinterpret_cast<char*>((reinterpret_cast<unsigned long>(top)) & -16LL);

	memset(&coctx_, 0, sizeof(coctx_));

	coctx_.regs[kRSP] = top;
	coctx_.regs[kRBP] = top;
	coctx_.regs[kRETAddr] = reinterpret_cast<char*>(CoFunction);
	coctx_.regs[kRDI] = reinterpret_cast<char*>(this);

	can_resume_ = true;

	return true;
}

Coroutine::~Coroutine() {
	t_coroutine_count--;
}

Coroutine* Coroutine::getCurrentCoroutine() {
	if (t_cur_coroutine == nullptr) {
		t_main_coroutine = new Coroutine();
		t_cur_coroutine = t_main_coroutine;
	}
	return t_cur_coroutine;
}

Coroutine* Coroutine::getMainCoroutine() {
	if (t_main_coroutine) {
		return t_main_coroutine;
	}
	t_main_coroutine = new Coroutine();
	return t_main_coroutine;
}

bool Coroutine::isMainCoroutine() {
	if (t_main_coroutine == nullptr || t_cur_coroutine == t_main_coroutine) {
		return true;
	}
	return false;
}

/********
从目标协程切回主协程
********/
void Coroutine::Yield() {
	// if (!t_enable_coroutine_swap) {
	//   ErrorLog << "can't yield, because disable coroutine swap";
	//   return;
	// }
	if (t_main_coroutine == nullptr) {
		ErrorLog << "main coroutine is nullptr";
		return;
	}

	if (t_cur_coroutine == t_main_coroutine) {
		ErrorLog << "current coroutine is main coroutine";
		return;
	}
	Coroutine* co = t_cur_coroutine;
	t_cur_coroutine = t_main_coroutine;
	t_cur_run_time = nullptr;
	coctx_swap(&(co->coctx_), &(t_main_coroutine->coctx_));
}

/********
从主协程切换到目标协程
********/
void Coroutine::Resume(Coroutine* co) {
	if (t_cur_coroutine != t_main_coroutine) {
		ErrorLog << "swap error, current coroutine must be main coroutine";
		return;
	}

	if (!t_main_coroutine) {
		ErrorLog << "main coroutine is nullptr";
		return;
	}
	if (!co || !co->can_resume_) {
		ErrorLog << "pending coroutine is nullptr or can_resume is false";
		return;
	}

	if (t_cur_coroutine == co) {
		DebugLog << "current coroutine is pending cor, need't swap";
		return;
	}

	t_cur_coroutine = co;
	t_cur_run_time = co->getRunTime();

	coctx_swap(&(t_main_coroutine->coctx_), &(co->coctx_));
}

}  // namespace crpc

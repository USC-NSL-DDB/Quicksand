#include "nu/utils/caladan.hpp"
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <typename Arg, typename Ret>
inline RobExecutor<Arg, Ret>::RobExecutor(
    std::move_only_function<Ret(const Arg &)> fn, uint32_t rob_size)
    : fn_(std::move(fn)),
      rob_(rob_size),
      done_(false),
      th_([&] { executor_fn(); }) {}

template <typename Arg, typename Ret>
RobExecutor<Arg, Ret>::~RobExecutor() {
  done_ = true;
  barrier();
  for (auto &entry : rob_) {
    entry.mutex.lock();
    entry.mutex.unlock();
    entry.cond_var.signal();
  }
  th_.join();
}

template <typename Arg, typename Ret>
Ret RobExecutor<Arg, Ret>::submit(uint32_t seq, Arg &&arg) {
  auto &entry = rob_[seq % rob_.size()];
  std::unique_ptr<Ret> ret_ptr;
  {
    ScopedLock<Mutex> lock(&entry.mutex);

    BUG_ON(entry.arg);
    auto arg_ptr = std::make_unique<Arg>(std::move(arg));
    entry.arg = std::move(arg_ptr);
    entry.cond_var.signal();
    while (!entry.ret) {
      entry.cond_var.wait(&entry.mutex);
      barrier();
    }
    ret_ptr = std::move(entry.ret);
  }

  return std::move(*ret_ptr);
}

template <typename Arg, typename Ret>
void RobExecutor<Arg, Ret>::executor_fn() {
  while (true) {
    for (auto &entry : rob_) {
      {
	ScopedLock<Mutex> lock(&entry.mutex);

        while (!entry.arg) {
          if (unlikely(Caladan::access_once(done_))) {
            return;
          }
          entry.cond_var.wait(&entry.mutex);
	  barrier();
        }
        auto arg_ptr = std::move(entry.arg);
        auto ret_ptr = std::make_unique<Ret>(fn_(*arg_ptr));
        entry.ret = std::move(ret_ptr);
      }
      entry.cond_var.signal();
    }
  }
}

}

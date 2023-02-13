#include "nu/utils/caladan.hpp"
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <typename Arg, typename Ret>
inline RobExecutor<Arg, Ret>::RobExecutor(
    std::move_only_function<Ret(Arg &&)> fn, uint32_t rob_size)
    : fn_(std::move(fn)), rob_(rob_size) {
  rob_[0].run = true;
}

template <typename Arg, typename Ret>
Ret RobExecutor<Arg, Ret>::submit(uint32_t seq, Arg &&arg) {
  auto *entry = &rob_[seq % rob_.size()];

  entry->mutex.lock();
  while (!entry->run) {
    entry->cond_var.wait(&entry->mutex);
  }
  auto ret = fn_(std::move(arg));
  entry->run = false;
  entry->mutex.unlock();

  auto *next_entry = (entry == &rob_.back()) ? &rob_.front() : entry + 1;
  next_entry->mutex.lock();
  next_entry->run = true;
  next_entry->mutex.unlock();
  next_entry->cond_var.signal();

  return ret;
}

}  // namespace nu

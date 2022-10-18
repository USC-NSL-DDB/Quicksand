#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/thread.hpp"

namespace nu {

template <typename Arg, typename Ret>
class RobExecutor {
 public:
  RobExecutor(std::function<Ret(const Arg &)> fn, uint32_t rob_size);
  ~RobExecutor();
  Ret submit(uint32_t seq, Arg &&arg);
  void executor_fn();

 private:
  struct RobEntry {
    std::unique_ptr<Arg> arg;
    std::unique_ptr<Ret> ret;
    Mutex mutex;
    CondVar cond_var;
  };

  std::function<Ret(const Arg &)> fn_;
  std::vector<RobEntry> rob_;
  Thread th_;
  bool done_;
};
}  // namespace nu

#include "nu/impl/rob_executor.ipp"

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
  RobExecutor(std::move_only_function<Ret(const Arg &)> fn, uint32_t rob_size);
  Ret submit(uint32_t seq, Arg &&arg);

 private:
  struct RobEntry {
    bool run = false;
    Mutex mutex;
    CondVar cond_var;
  };

  std::move_only_function<Ret(const Arg &)> fn_;
  std::vector<RobEntry> rob_;
};
}  // namespace nu

#include "nu/impl/rob_executor.ipp"

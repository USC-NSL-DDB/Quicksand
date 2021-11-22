#pragma once

#include "nu/commons.hpp"
#include "nu/utils/robin_hood.h"
#include "nu/utils/spinlock.hpp"

namespace nu {

class ThreadSet {
public:
  void put(thread_t *th);
  void put(thread_t *th, int8_t core_id);
  void remove(thread_t *th);
  std::vector<thread_t *> all_threads();

private:
  struct PerCoreData {
    robin_hood::unordered_set<thread_t *> set;
    SpinLock spin;
  } data_[kNumCores];
};

} // namespace nu

#include "nu/impl/thread_set.ipp"

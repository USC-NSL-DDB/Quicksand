#pragma once

#include <cstdint>

#include <sync.h>

#include "nu/commons.hpp"
#include "nu/utils/rcu_lock.hpp"

namespace nu {

class StackManager {
public:
  StackManager(VAddrRange stack_cluster);
  uint8_t *get();
  void put(uint8_t *stack);
  VAddrRange get_range();

  static void mmap(VAddrRange stack_cluster);
  static void munmap(VAddrRange stack_cluster);

private:
  struct alignas(kCacheLineBytes) CoreCache {
    uint8_t *stack;
  };

  VAddrRange range_;
  uint64_t global_pool_size_;
  uint8_t *global_pool_[kMaxNumStacksPerCluster];
  CoreCache core_caches_[kNumCores];
  rt::Spin spin_;
  friend class Test;
};

} // namespace nu

#include "nu/impl/stack_manager.ipp"

#pragma once

#include <cstdint>
#include <map>
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
  void add_ref_cnt(VAddrRange borrowed_stack_cluster, uint32_t cnt);

private:
  struct alignas(kCacheLineBytes) CoreCache {
    uint8_t *stack;
  };

  VAddrRange range_;
  uint64_t global_pool_size_;
  uint8_t *global_pool_[kMaxNumStacksPerCluster];
  CoreCache core_caches_[kNumCores];
  std::map<VAddrRange, uint32_t> borrowed_stack_ref_cnt_map_;
  rt::Mutex mutex_;

  static void mmap(VAddrRange borrowed_stack_cluster);
  static void munmap(VAddrRange borrowed_stack_cluster);
};

} // namespace nu

#include "nu/impl/stack_manager.ipp"

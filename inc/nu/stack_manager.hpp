#pragma once

#include <cstdint>
#include <map>
#include <sync.h>

#include "nu/commons.hpp"
#include "nu/utils/rcu_lock.hpp"
#include "nu/utils/cached_pool.hpp"

namespace nu {

class StackManager {
public:
  constexpr static uint32_t kPerCoreCacheSize = 32;

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
  CachedPool<uint8_t> cached_pool_;
  std::map<VAddrRange, uint32_t> borrowed_stack_ref_cnt_map_;
  rt::Mutex mutex_;

  static void mmap(VAddrRange borrowed_stack_cluster);
  static void munmap(VAddrRange borrowed_stack_cluster);
};

} // namespace nu

#include "nu/impl/stack_manager.ipp"

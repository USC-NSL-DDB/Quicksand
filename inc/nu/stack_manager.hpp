#pragma once

#include <sync.h>

#include <cstdint>
#include <map>

#include "nu/commons.hpp"
#include "nu/utils/cached_pool.hpp"
#include "nu/utils/rcu_lock.hpp"

namespace nu {

class StackManager {
 public:
  constexpr static uint32_t kPerCoreCacheSize = 32;

  StackManager(VAddrRange stack_cluster);
  uint8_t *get();
  void put(uint8_t *stack);

 private:
  struct alignas(kCacheLineBytes) CoreCache {
    uint8_t *stack;
  };

  VAddrRange range_;
  CachedPool<uint8_t> cached_pool_;
  rt::Mutex mutex_;

  bool not_in_range(uint8_t *stack);
  void release_space(uint8_t *stack);
};

}  // namespace nu


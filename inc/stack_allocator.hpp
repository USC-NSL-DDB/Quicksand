#pragma once

#include <cstdint>

#include <sync.h>

#include "defs.hpp"

namespace nu {
class StackAllocator {
public:
  StackAllocator();
  StackAllocator(uint8_t *base, uint64_t num_stacks);
  void init(uint8_t *base, uint64_t num_stacks);
  uint8_t *get();
  void put(uint8_t *stack);

private:
  struct alignas(kCacheLineBytes) CoreCache {
    uint8_t *stack;
  };

  uint64_t global_pool_size_;
  uint8_t *global_pool_[kMaxNumStacksPerHeap];
  CoreCache core_caches_[kNumCores];
  rt::Spin spin_;
};
} // namespace nu


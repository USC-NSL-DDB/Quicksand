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

  uint64_t num_stacks_;
  uint64_t global_pool_size_;
  uint64_t num_touched_; // For profiling.
  uint8_t *global_pool_[kMaxNumStacksPerHeap];
  CoreCache core_caches_[kNumCores];
  rt::Spin spin_;
  friend class Test;
};
} // namespace nu


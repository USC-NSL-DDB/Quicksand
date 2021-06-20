#include <algorithm>
#include <cstring>

extern "C" {
#include <runtime/preempt.h>
}

#include "stack_allocator.hpp"

namespace nu {

StackAllocator::StackAllocator() {}

StackAllocator::StackAllocator(uint8_t *base, uint64_t num_stacks) {
  init(base, num_stacks);
}

void StackAllocator::init(uint8_t *base, uint64_t num_stacks) {
  num_stacks_ = num_stacks;
  global_pool_size_ = num_stacks;
  for (uint64_t i = 0; i < num_stacks; i++) {
    global_pool_[i] = base;
    base += kStackSize;
  }
  for (uint64_t i = 0; i < kNumCores; i++) {
    core_caches_[i].stack = nullptr;
  }
}

uint8_t *StackAllocator::get() {
  int core = get_cpu();
  auto &core_cache = core_caches_[core];
  if (likely(core_cache.stack)) {
    auto ret = core_cache.stack;
    core_cache.stack = nullptr;
    put_cpu();
    return ret;
  }
  put_cpu();
  rt::ScopedLock<rt::Spin> lock(&spin_);
  BUG_ON(!global_pool_size_); // Run out of stack.
  auto ret = global_pool_[--global_pool_size_];
  num_touched_ = std::max(num_touched_, num_stacks_ - global_pool_size_);
  return ret;
}

void StackAllocator::put(uint8_t *stack) {
  int core = get_cpu();
  auto &core_cache = core_caches_[core];
  if (likely(!core_cache.stack)) {
    core_cache.stack = stack;
    put_cpu();
    return;
  }
  put_cpu();
  rt::ScopedLock<rt::Spin> lock(&spin_);
  global_pool_[global_pool_size_++] = stack;
}

} // namespace nu

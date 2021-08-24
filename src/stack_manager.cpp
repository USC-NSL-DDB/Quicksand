#include <algorithm>
#include <cstring>
#include <sys/mman.h>

extern "C" {
#include <runtime/preempt.h>
}

#include "nu/stack_manager.hpp"

namespace nu {

StackManager::StackManager(VAddrRange stack_cluster) : range_(stack_cluster) {
  mmap(stack_cluster);
  auto num_stacks = (stack_cluster.end - stack_cluster.start) / kStackSize -
                    1; // The first kStackSize bytes are reserved for metadata.
  auto *ptr = reinterpret_cast<uint8_t *>(stack_cluster.start + kStackSize);
  global_pool_size_ = num_stacks;
  for (uint64_t i = 0; i < num_stacks; i++) {
    global_pool_[i] = ptr;
    ptr += kStackSize;
  }
  for (uint64_t i = 0; i < kNumCores; i++) {
    core_caches_[i].stack = nullptr;
  }
}

void StackManager::mmap(VAddrRange stack_cluster) {
  auto stack_cluster_start_ptr =
      reinterpret_cast<uint8_t *>(stack_cluster.start);
  auto mmap_addr = ::mmap(
      stack_cluster_start_ptr, stack_cluster.end - stack_cluster.start,
      PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != stack_cluster_start_ptr);
}

void StackManager::munmap(VAddrRange stack_cluster) {
  auto stack_cluster_start_ptr =
      reinterpret_cast<uint8_t *>(stack_cluster.start);
  BUG_ON(::munmap(stack_cluster_start_ptr,
                  stack_cluster.end - stack_cluster.start) != 0);
}

void StackManager::add_ref_cnt(VAddrRange borrowed_stack_cluster,
                               uint32_t cnt) {
  rt::ScopedLock<rt::Mutex> guard(&mutex_);
  auto &ref_cnt = borrowed_stack_ref_cnt_map_[borrowed_stack_cluster];
  if (!ref_cnt && cnt > 0) {
    mmap(borrowed_stack_cluster);
  }
  ref_cnt += cnt;
  if (!ref_cnt && cnt < 0) {
    munmap(borrowed_stack_cluster);
  }
}

uint8_t *StackManager::get() {
  int core = get_cpu();
  auto &core_cache = core_caches_[core];
  if (likely(core_cache.stack)) {
    auto ret = core_cache.stack;
    core_cache.stack = nullptr;
    put_cpu();
    return ret;
  }
  put_cpu();
  rt::ScopedLock<rt::Mutex> guard(&mutex_);
  BUG_ON(!global_pool_size_); // Run out of stack.
  auto ret = global_pool_[--global_pool_size_];
  return ret;
}

void StackManager::put(uint8_t *stack) {
  int core = get_cpu();
  auto &core_cache = core_caches_[core];
  if (likely(!core_cache.stack)) {
    core_cache.stack = stack;
    put_cpu();
    return;
  }
  put_cpu();
  rt::ScopedLock<rt::Mutex> guard(&mutex_);
  global_pool_[global_pool_size_++] = stack;
}

} // namespace nu

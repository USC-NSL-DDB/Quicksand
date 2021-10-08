#include <algorithm>
#include <cstring>
#include <sys/mman.h>

extern "C" {
#include <runtime/preempt.h>
}

#include "nu/stack_manager.hpp"

namespace nu {

StackManager::StackManager(VAddrRange stack_cluster)
    : range_(stack_cluster), cached_pool_([]() -> uint8_t * { BUG(); },
                                          [](uint8_t *) {}, kPerCoreCacheSize) {
  mmap(stack_cluster);
  auto num_stacks = (stack_cluster.end - stack_cluster.start) / kStackSize -
                    1; // The first kStackSize bytes are reserved for metadata.
  auto *ptr = reinterpret_cast<uint8_t *>(stack_cluster.start + kStackSize);
  for (uint64_t i = 0; i < num_stacks; i++) {
    cached_pool_.put(ptr);
    ptr += kStackSize;
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

uint8_t *StackManager::get() { return cached_pool_.get(); }

void StackManager::put(uint8_t *stack) { cached_pool_.put(stack); }

} // namespace nu

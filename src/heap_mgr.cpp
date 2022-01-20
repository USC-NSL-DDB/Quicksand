#include <cstdint>
#include <functional>
#include <memory>

extern "C" {
#include <base/assert.h>
#include <runtime/thread.h>
}

#include "nu/heap_mgr.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

namespace nu {

HeapManager::HeapManager() {
  num_present_heaps_ = 0;
  for (uint64_t vaddr = kMinHeapVAddr; vaddr + kHeapSize <= kMaxHeapVAddr;
       vaddr += kHeapSize) {
    auto *heap_base = reinterpret_cast<HeapHeader *>(vaddr);
    auto mmap_addr =
        ::mmap(heap_base, kNumAlwaysMmapedBytes, PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, -1, 0);
    auto *heap_header = reinterpret_cast<HeapHeader *>(mmap_addr);
    heap_header->status = kAbsent;
    std::construct_at(&heap_header->rcu_lock);
    std::construct_at(&heap_header->spin_lock);
    std::construct_at(&heap_header->cond_var);
  }
}

void HeapManager::allocate(void *heap_base, bool migratable) {
  auto mmap_addr =
      ::mmap(reinterpret_cast<uint8_t *>(heap_base) + kNumAlwaysMmapedBytes,
             kHeapSize - kNumAlwaysMmapedBytes, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr == reinterpret_cast<void *>(-1));
  setup(heap_base, migratable, /* from_migration = */ false);
}

void HeapManager::mmap_populate(void *heap_base, uint64_t populate_len) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  auto *mmap_base =
      reinterpret_cast<uint8_t *>(heap_base) + kNumAlwaysMmapedBytes;
  auto total_mmap_size = kHeapSize - kNumAlwaysMmapedBytes;
  populate_len -= kNumAlwaysMmapedBytes;
  populate_len = ((populate_len - 1) / kPageSize + 1) * kPageSize;
  auto mmap_addr =
      ::mmap(mmap_base, populate_len, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, -1, 0);
  BUG_ON(mmap_addr != mmap_base);
  auto *unpopulated_base = mmap_base + populate_len;
  mmap_addr = ::mmap(unpopulated_base, total_mmap_size - populate_len,
                     PROT_READ | PROT_WRITE,
                     MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != unpopulated_base);
  heap_header->status = kMapped;
}

void HeapManager::deallocate(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  heap_header->spin_lock.lock(); // Sync with PressureHandler.

  heap_header->status = kAbsent;
  auto *munmap_base =
      reinterpret_cast<uint8_t *>(heap_base) + kNumAlwaysMmapedBytes;
  auto total_munmap_size = kHeapSize - kNumAlwaysMmapedBytes;
  RuntimeSlabGuard guard;
  std::destroy_at(&heap_header->blocked_syncer);
  std::destroy_at(&heap_header->time);
  // FIXME
  // std::destroy_at(&heap_header->migrated_wg);
  std::destroy_at(&heap_header->spin);
  std::destroy_at(&heap_header->slab);
  BUG_ON(munmap(munmap_base, total_munmap_size) == -1);
  heap_header->spin_lock.unlock();
}

void HeapManager::setup(void *heap_base, bool migratable, bool from_migration) {
  RuntimeSlabGuard guard;
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);

  std::construct_at(&heap_header->blocked_syncer);
  std::construct_at(&heap_header->time);
  heap_header->migratable = migratable;
  // FIXME
  // std::construct_at(&heap_header->migrated_wg);

  if (!from_migration) {
    std::construct_at(&heap_header->spin);
    heap_header->ref_cnt = 1;
    auto heap_region_size = kHeapSize - sizeof(HeapHeader);
    heap_header->slab.init(to_slab_id(heap_header), heap_header + 1,
                           heap_region_size);
  }
}

std::vector<void *> HeapManager::get_all_heaps() {
  {
    rt::SpinGuard guard(&spin_);
    auto iter = present_heaps_.begin();
    for (auto *heap_base : present_heaps_) {
      auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
      if (heap_header->status == kPresent) {
        *iter = heap_base;
        iter++;
      }
    }
    present_heaps_.erase(iter, present_heaps_.end());
  }
  return present_heaps_;
}

uint64_t HeapManager::get_mem_usage() {
  uint64_t total_mem_usage = 0;
  auto heaps = get_all_heaps();
  for (auto *heap_base : heaps) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    auto &heap_slab = heap_header->slab;
    total_mem_usage += reinterpret_cast<uint8_t *>(heap_slab.get_base()) -
                       reinterpret_cast<uint8_t *>(heap_header) +
                       heap_slab.get_usage();
  }

  return total_mem_usage;
}

} // namespace nu

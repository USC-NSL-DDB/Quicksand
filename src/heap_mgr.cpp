#include <cstdint>
#include <functional>
#include <memory>

extern "C" {
#include <base/assert.h>
#include <runtime/thread.h>
}

#include "nu/cond_var.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/monitor.hpp"
#include "nu/mutex.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

namespace nu {

HeapManager::HeapManager() {
  for (uint64_t vaddr = kMinHeapVAddr; vaddr + kHeapSize <= kMaxHeapVAddr;
       vaddr += kHeapSize) {
    auto *heap_base = reinterpret_cast<HeapHeader *>(vaddr);
    auto mmap_addr =
        ::mmap(heap_base, kPageSize, PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, -1, 0);
    auto *heap_header = reinterpret_cast<HeapHeader *>(mmap_addr);
    heap_header->present = false;
    std::construct_at(&heap_header->rcu_lock);
  }
}

void HeapManager::mmap_populate(void *heap_base, uint64_t populate_len) {
  auto *mmap_base = reinterpret_cast<uint8_t *>(heap_base) + kPageSize;
  auto total_mmap_size = kHeapSize - kPageSize;
  populate_len -= kPageSize;
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
}

void HeapManager::deallocate(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  auto *munmap_base = reinterpret_cast<uint8_t *>(heap_base) + kPageSize;
  auto total_munmap_size = kHeapSize - kPageSize;
  RuntimeHeapGuard guard;
  heap_header->threads.reset();
  heap_header->mutexes.reset();
  heap_header->condvars.reset();
  heap_header->time.reset();
  std::destroy_at(&heap_header->forward_wg);
  std::destroy_at(&heap_header->spin);
  std::destroy_at(&heap_header->slab);
  BUG_ON(munmap(munmap_base, total_munmap_size) == -1);
}

void HeapManager::setup(void *heap_base, bool migratable, bool from_migration) {
  RuntimeHeapGuard guard;
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);

  heap_header->threads.release();
  heap_header->threads.reset(
      new decltype(heap_header->threads)::element_type());

  heap_header->mutexes.release();
  heap_header->mutexes.reset(
      new decltype(heap_header->mutexes)::element_type());

  heap_header->condvars.release();
  heap_header->condvars.reset(
      new decltype(heap_header->condvars)::element_type());

  heap_header->time.release();
  heap_header->time.reset(new decltype(heap_header->time)::element_type());

  heap_header->migratable = migratable;

  std::construct_at(&heap_header->forward_wg);

  if (!from_migration) {
    std::construct_at(&heap_header->spin);
    heap_header->ref_cnt = 1;
    auto heap_region_size = kHeapSize - sizeof(HeapHeader);
    heap_header->slab.init(to_u16(heap_header), heap_header + 1,
                           heap_region_size);
  }
}

std::vector<HeapRange> HeapManager::pick_heaps(uint32_t min_num_heaps,
                                               uint32_t min_mem_mbs) {
  std::vector<HeapRange> heaps;
  uint32_t picked_heaps_mem_mbs = 0;
  rt::MutexGuard guard(&mutex_);

  for (auto *heap_base : present_heaps_) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    if (heap_header->migratable) {
      auto &slab = heap_header->slab;
      uint64_t len = reinterpret_cast<uint64_t>(slab.get_base()) +
                     slab.get_usage() - reinterpret_cast<uint64_t>(heap_header);

      HeapRange range{heap_header, len};
      heaps.push_back(range);

      picked_heaps_mem_mbs += len / kOneMB;
      if (picked_heaps_mem_mbs >= min_mem_mbs &&
          heaps.size() >= min_num_heaps) {
        break;
      }
    }
  }

  return heaps;
}

uint64_t HeapManager::get_mem_usage() {
  uint64_t total_mem_usage = 0;
  rt::MutexGuard guard(&mutex_);

  for (auto *heap_base : present_heaps_) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    auto &heap_slab = heap_header->slab;
    total_mem_usage += reinterpret_cast<uint8_t *>(heap_slab.get_base()) -
                       reinterpret_cast<uint8_t *>(heap_header) +
                       heap_slab.get_usage();
  }

  return total_mem_usage;
}

} // namespace nu

#include <cstdint>
#include <functional>

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

void HeapManager::mmap_populate(void *heap_base, uint64_t populate_len) {
  populate_len = ((populate_len - 1) / kPageSize + 1) * kPageSize;
  BUG_ON(populate_len > kHeapSize);
  auto mmap_addr =
      ::mmap(heap_base, populate_len, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, -1, 0);
  BUG_ON(mmap_addr != heap_base);
  auto *unpopulated_base =
      reinterpret_cast<uint8_t *>(heap_base) + populate_len;
  mmap_addr =
      ::mmap(unpopulated_base, kHeapSize - populate_len, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != unpopulated_base);
}

void HeapManager::setup(void *heap_base, bool migratable, bool from_migration) {
  auto heap_header = reinterpret_cast<HeapHeader *>(heap_base);

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

  new (&heap_header->forward_wg) rt::WaitGroup();

  if (!from_migration) {
    new (&heap_header->spin) rt::Spin();
    heap_header->ref_cnt = 1;
    auto heap_region_size = kHeapSize - sizeof(HeapHeader);
    heap_header->slab.init(to_u16(heap_header), heap_header + 1,
                           heap_region_size);
  }
}

std::vector<HeapRange> HeapManager::pick_heaps(const Resource &pressure) {
  std::vector<HeapRange> heaps;
  std::function fn = [&, pressure = pressure](
                         const std::pair<HeapHeader *const, bool> &p) mutable {
    auto *heap_header = p.first;
    if (heap_header->migratable) {
      auto &slab = heap_header->slab;
      uint64_t len = reinterpret_cast<uint64_t>(slab.get_base()) +
                     slab.get_usage() - reinterpret_cast<uint64_t>(heap_header);
      HeapRange range{heap_header, len};
      heaps.push_back(range);
      auto len_in_mbs = len >> 20;
      // TODO: also consider CPU pressure.
      if (pressure.mem_mbs <= len_in_mbs) {
        return false;
      }
      pressure.mem_mbs -= len_in_mbs;
    }
    return true;
  };

  active_heaps_->for_each(fn);
  return heaps;
}

bool HeapManager::migration_disable_initial(HeapHeader *heap_header) {
  std::function fn = [](std::pair<HeapHeader *const, bool> *p) {
    if (!p) {
      return false;
    }
    p->first->rcu_lock.reader_lock();
    return true;
  };
  return active_heaps_->apply(heap_header, fn);
}

void HeapManager::migration_disable(HeapHeader *heap_header) {
  auto migrating = !migration_disable_initial(heap_header);
  if (unlikely(migrating)) {
    while (!thread_is_migrated()) {
      rt::Yield();
    }
  }
  heap_header->threads->remove(thread_self());
}

} // namespace nu

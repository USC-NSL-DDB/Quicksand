#include <cstdint>
#include <functional>

extern "C" {
#include <base/assert.h>
#include <runtime/thread.h>
}

#include "heap_mgr.hpp"
#include "monitor.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"

namespace nu {

void HeapManager::allocate(void *heap_base) {
  auto mmap_addr = mmap(heap_base, kHeapSize, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != heap_base);
  BUG_ON(madvise(mmap_addr, kHeapSize, MADV_HUGEPAGE) != 0);
  auto *heap_header = new (heap_base) HeapHeader();
  heap_header->ref_cnt = 0;
  heap_header->threads.reset(
      new decltype(heap_header->threads)::element_type());
  heap_header->slab.init(heap_header + 1, kHeapSize - sizeof(HeapHeader));
}

std::list<void *> HeapManager::pick_heaps(const Resource &pressure) {
  std::list<void *> heaps;
  std::function<bool(void *const &)> fn =
      [&, pressure = pressure](void *const &heap_base) mutable {
        heaps.push_back(heap_base);
        auto slab = get_slab(heap_base);
        auto heap_usage = slab->get_usage() >> 20;
        // TODO: also consider CPU pressure.
        if (pressure.mem_mbs <= heap_usage) {
          return false;
        }
        pressure.mem_mbs -= heap_usage;
        return true;
      };
  heap_statuses_->for_each(fn);
  return heaps;
}

} // namespace nu

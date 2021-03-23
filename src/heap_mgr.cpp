#include <cstdint>
#include <functional>
#include <sys/mman.h>

extern "C" {
#include <base/assert.h>
#include <runtime/thread.h>
}

#include "heap_mgr.hpp"
#include "monitor.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"

namespace nu {

HeapManager::HeapManager()
    : heap_statuses_(new decltype(heap_statuses_)::element_type()) {}

void HeapManager::allocate(void *heap_base) {
  auto mmap_addr = mmap(heap_base, kHeapSize, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != heap_base);
  BUG_ON(madvise(mmap_addr, kHeapSize, MADV_HUGEPAGE) != 0);
  auto *heap_header = new (heap_base) HeapHeader();
  heap_header->threads.reset(
      new decltype(heap_header->threads)::element_type());
  heap_header->slab.init(heap_header + 1, kHeapSize - sizeof(HeapHeader));
  heap_statuses_->put(heap_base);
}

void HeapManager::free(void *heap_base) {
  BUG_ON(munmap(heap_base, kHeapSize) == -1);
  heap_statuses_->remove(heap_base);
}

SlabAllocator *HeapManager::get_slab(void *heap_base) {
  return &(reinterpret_cast<HeapHeader *>(heap_base)->slab);
}

std::vector<void *> HeapManager::pick_heaps(const Pressure &pressure) {
  std::vector<void *> heaps;
  std::function<bool(void *const &)> fn =
      [&, pressure = pressure](void *const &heap_base) mutable {
        heaps.push_back(heap_base);
        auto slab = get_slab(heap_base);
        auto heap_usage = slab->get_usage();
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

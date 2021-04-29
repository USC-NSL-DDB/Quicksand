#include <cstdint>
#include <functional>

extern "C" {
#include <base/assert.h>
#include <runtime/thread.h>
}

#include "cond_var.hpp"
#include "heap_mgr.hpp"
#include "monitor.hpp"
#include "mutex.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"

namespace nu {

void HeapManager::allocate(void *heap_base, bool migratable) {
  mmap(heap_base);
  setup(heap_base, migratable, /* skip_slab = */ false);
}

void HeapManager::mmap(void *heap_base) {
  auto mmap_addr = ::mmap(heap_base, kHeapSize, PROT_READ | PROT_WRITE,
                          MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != heap_base);
}

void HeapManager::setup(void *heap_base, bool migratable, bool skip_slab) {
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
  new (&heap_header->spin) rt::Spin();
  heap_header->ref_cnt = 1;
  if (!skip_slab) {
    uint16_t sentinel = reinterpret_cast<uint64_t>(heap_base) / kHeapSize;
    heap_header->slab.init(sentinel, heap_header + 1,
                           kHeapSize - sizeof(HeapHeader));
  }
}

std::list<void *> HeapManager::pick_heaps(const Resource &pressure) {
  std::list<void *> heaps;
  std::function<bool(HeapHeader *const &)> fn =
      [&, pressure = pressure](HeapHeader *const &heap_header) mutable {
	if (heap_header->migratable) {
          heaps.push_back(heap_header);
          auto slab = get_slab(heap_header);
          auto heap_usage = slab->get_usage() >> 20;
          // TODO: also consider CPU pressure.
          if (pressure.mem_mbs <= heap_usage) {
            return false;
          }
          pressure.mem_mbs -= heap_usage;
        }
        return true;
      };

  heap_statuses_->for_each(fn);
  return heaps;
}

} // namespace nu

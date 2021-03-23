#pragma once

#include <cstdint>
#include <memory>
#include <unordered_set>

extern "C" {
#include <runtime/thread.h>
}

#include "defs.hpp"
#include "sync.h"
#include "utils/rcu_lock.hpp"
#include "utils/slab.hpp"
#include "utils/ts_hash_set.hpp"

namespace nu {

struct Pressure;
class SlabAllocator;
template <typename T> class RuntimeAllocator;

struct HeapHeader {
  RCULock rcu_lock;
  bool migrating;
  std::unique_ptr<ThreadSafeHashSet<thread_t *, RuntimeAllocator<thread_t *>>>
      threads;
  SlabAllocator slab;
};

class HeapManager {
public:
  constexpr static uint64_t kHeapSize = 0x40000000ULL;

  HeapManager();
  void allocate(void *heap_base);
  void free(void *heap_base);
  SlabAllocator *get_slab(void *heap_base);
  std::vector<void *> pick_heaps(const Pressure &pressure);

private:
  std::unique_ptr<ThreadSafeHashSet<void *, RuntimeAllocator<void *>>>
      heap_statuses_;
};
} // namespace nu

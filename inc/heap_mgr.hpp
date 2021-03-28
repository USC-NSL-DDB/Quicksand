#pragma once

#include <cstdint>
#include <functional>
#include <list>
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
  // Migration related.
  std::unique_ptr<ThreadSafeHashSet<thread_t *, RuntimeAllocator<thread_t *>>>
      threads;

  // Ref cnt related.
  rt::Spin spin;
  int ref_cnt;

  // Heap mem allocator.
  SlabAllocator slab;
};

class HeapManager {
public:
  constexpr static uint64_t kHeapSize = 0x40000000ULL;

  HeapManager();
  static void allocate(void *heap_base);
  static void deallocate(void *heap_base);
  void insert(void *heap_base);
  bool contains(void *heap_base);
  bool remove(void *heap_base);
  void rcu_lock();
  void rcu_unlock();
  void rcu_synchronize();
  SlabAllocator *get_slab(void *heap_base);
  std::list<void *> pick_heaps(const Resource &pressure);

private:
  std::unique_ptr<ThreadSafeHashSet<void *, RuntimeAllocator<void *>>>
      heap_statuses_;
  RCULock rcu_lock_;
};
} // namespace nu

#include "impl/heap_mgr.ipp"

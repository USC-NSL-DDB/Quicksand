#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <unordered_set>

extern "C" {
#include <runtime/net.h>
#include <runtime/thread.h>
}
#include <sync.h>

#include "defs.hpp"
#include "stack_allocator.hpp"
#include "utils/rcu_hash_set.hpp"
#include "utils/rcu_lock.hpp"
#include "utils/refcount_hash_set.hpp"
#include "utils/slab.hpp"

namespace nu {

struct Pressure;
class SlabAllocator;
class Mutex;
class CondVar;
class Time;
template <typename T> class RuntimeAllocator;

struct HeapHeader {
  ~HeapHeader();

  // Migration related.
  std::unique_ptr<RefcountHashSet<thread_t *, RuntimeAllocator<thread_t *>>>
      threads;
  std::unique_ptr<RefcountHashSet<Mutex *, RuntimeAllocator<Mutex *>>> mutexes;
  std::unique_ptr<RefcountHashSet<CondVar *, RuntimeAllocator<CondVar *>>>
      condvars;
  std::unique_ptr<Time> time;
  bool migratable;
  bool migrating;

  // Forwarding related.
  uint32_t old_server_ip;
  rt::WaitGroup forward_wg;

  // Ref cnt related.
  rt::Spin spin;
  int ref_cnt;

  // Stack allocator.
  StackAllocator stack_allocator;

  // Heap Mem allocator. Must be the last field.
  SlabAllocator slab;
};

class HeapManager {
public:
  constexpr static uint64_t kHeapSize = 0x40000000ULL;

  HeapManager();
  static void allocate(void *heap_base, bool migratable);
  static void mmap(void *heap_base);
  static void mmap_populate(void *heap_base, uint64_t populate_len);
  static void setup(void *heap_base, bool migratable, bool from_migration);
  static void deallocate(void *heap_base);
  void insert(void *heap_base);
  bool contains(void *heap_base);
  bool remove(void *heap_base);
  void rcu_reader_lock();
  bool rcu_reader_lock_np();
  void rcu_reader_unlock();
  void rcu_reader_unlock_np();
  void rcu_writer_sync();
  std::list<void *> pick_heaps(const Resource &pressure);

private:
  std::unique_ptr<RCUHashSet<HeapHeader *, RuntimeAllocator<HeapHeader *>>>
      heap_statuses_;
  RCULock rcu_lock_;
};
} // namespace nu

#include "impl/heap_mgr.ipp"

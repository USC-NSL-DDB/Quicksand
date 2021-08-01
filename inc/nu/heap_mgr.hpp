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

#include "nu/commons.hpp"
#include "nu/utils/rcu_lock.hpp"
#include "nu/utils/refcount_hash_set.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

struct Pressure;
class SlabAllocator;
class Mutex;
class CondVar;
class Time;
template <typename T> class RuntimeAllocator;
template <typename K, typename V, typename Allocator> class RCUHashMap;

enum HeapStatus { PRESENT, MIGRATING };

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
  RCULock rcu_lock;

  // Forwarding related.
  uint32_t old_server_ip;
  rt::WaitGroup forward_wg;

  // Ref cnt related.
  rt::Spin spin;
  int ref_cnt;

  // Heap Mem allocator. Must be the last field.
  SlabAllocator slab;
};

class HeapManager {
public:
  HeapManager();
  static void allocate(void *heap_base, bool migratable);
  static void mmap(void *heap_base);
  static void mmap_populate(void *heap_base, uint64_t populate_len);
  static void setup(void *heap_base, bool migratable, bool from_migration);
  static void deallocate(void *heap_base);
  HeapStatus *get_status(void *heap_base);
  void insert(void *heap_base);
  bool contains(void *heap_base);
  bool is_present(void *heap_base);
  bool remove(void *heap_base);
  bool remove_if_not_migrating(void *heap_base);
  bool mark_migrating(void *heap_base);
  bool migration_disable_initial(HeapHeader *heap_header);
  void migration_enable_final(HeapHeader *heap_header);
  static void migration_disable();
  static void migration_enable();
  std::vector<HeapRange> pick_heaps(const Resource &pressure);

private:
  std::unique_ptr<
      RCUHashMap<HeapHeader *, HeapStatus,
                 RuntimeAllocator<std::pair<HeapHeader *const, HeapStatus>>>>
      heap_statuses_;
  RCULock rcu_lock_;
  friend class Test;
};
} // namespace nu

#include "nu/impl/heap_mgr.ipp"

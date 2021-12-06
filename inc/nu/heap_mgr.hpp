#pragma once

#include <cstddef>
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
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/cpu_load.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/rcu_lock.hpp"
#include "nu/utils/slab.hpp"
#include "nu/utils/spinlock.hpp"
#include "nu/utils/time.hpp"

namespace nu {

struct Pressure;
class SlabAllocator;
class Mutex;
class CondVar;
class Time;
template <typename T> class RuntimeAllocator;

struct HeapHeader {
  ~HeapHeader();

  bool present;

  // For synchronization on migration.
  RCULock rcu_lock;
  Mutex mutex;
  SpinLock spin_lock;
  CondVar cond_var;

  // Used for monitoring cpu load.
  CPULoad cpu_load;

  //--- Fields above are always mmaped in all object servers. ---/
  uint8_t always_mmaped_end[0];

  // Migration related.
  BlockedSyncer blocked_syncer;
  Time time;
  bool migratable;

  // Forwarding related.
  // FIXME
  // rt::WaitGroup migrated_wg;

  //--- Fields below will be automatically copied during migration. ---/
  uint8_t copy_start[0];

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
  void insert(void *heap_base);
  bool remove(void *heap_base);
  bool remove_with_present(void *heap_base);
  void mark_absent(void *heap_base);
  std::vector<void *> get_all_heaps();
  std::unordered_set<void *> &acquire_all_heaps();
  void release_all_heaps();
  uint64_t get_mem_usage();
  uint32_t get_num_present_heaps();

private:
  constexpr static uint32_t kNumAlwaysMmapedPages =
      (offsetof(HeapHeader, always_mmaped_end) - 1) / kPageSize + 1;
  constexpr static uint32_t kNumAlwaysMmapedBytes =
      kNumAlwaysMmapedPages * kPageSize;

  std::unordered_set<void *> present_heaps_;
  rt::Spin spin_;
  friend class MigrationEnabledGuard;
  friend class MigrationDisabledGuard;
  friend class NonBlockingMigrationDisabledGuard;
  friend class Test;

  bool try_disable_migration(HeapHeader *heap_header);
  void disable_migration(HeapHeader *heap_header);
  static void enable_migration(HeapHeader *heap_header);
};

class MigrationEnabledGuard {
public:
  // By default guards the current object header.
  MigrationEnabledGuard();
  MigrationEnabledGuard(HeapHeader *heap_header);
  MigrationEnabledGuard(MigrationEnabledGuard &&o);
  MigrationEnabledGuard &operator=(MigrationEnabledGuard &&o);
  void reset();
  ~MigrationEnabledGuard();

private:
  HeapHeader *heap_header_;
};

class MigrationDisabledGuard {
public:
  // By default guards the current object header.
  MigrationDisabledGuard();
  MigrationDisabledGuard(HeapHeader *heap_header);
  MigrationDisabledGuard(MigrationDisabledGuard &&o);
  MigrationDisabledGuard &operator=(MigrationDisabledGuard &&o);
  void reset();
  ~MigrationDisabledGuard();
  operator bool() const;
  HeapHeader *get_heap_header();

private:
  HeapHeader *heap_header_;
};

class NonBlockingMigrationDisabledGuard {
public:
  // By default guards the current object header.
  NonBlockingMigrationDisabledGuard();
  NonBlockingMigrationDisabledGuard(HeapHeader *heap_header);
  NonBlockingMigrationDisabledGuard(NonBlockingMigrationDisabledGuard &&o);
  NonBlockingMigrationDisabledGuard &
  operator=(NonBlockingMigrationDisabledGuard &&o);
  ~NonBlockingMigrationDisabledGuard();
  void reset();
  operator bool() const;
  HeapHeader *get_heap_header();

private:
  HeapHeader *heap_header_;
};

} // namespace nu

#include "nu/impl/heap_mgr.ipp"

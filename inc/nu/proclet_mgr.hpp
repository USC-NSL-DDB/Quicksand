#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <vector>

extern "C" {
#include <runtime/net.h>
#include <runtime/thread.h>
}
#include <sync.h>

#include "nu/commons.hpp"
#include "nu/utils/blocked_syncer.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/counter.hpp"
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
template <typename T>
class RuntimeAllocator;

enum ProcletStatus { kAbsent = 0, kMapped, kPresent, kDestructed };

struct ProcletHeader {
  ~ProcletHeader();

  uint8_t status;

  // For synchronization.
  RCULock rcu_lock;
  SpinLock spin_lock;
  CondVar cond_var;

  // Used for monitoring cpu load.
  CPULoad cpu_load;

  // Used for monitoring active threads count.
  Counter thread_cnt;

  //--- Fields above are always mmaped in all proclet servers. ---/
  uint8_t always_mmaped_end[0];

  // Migration related.
  std::atomic<int8_t> pending_load_cnt;
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

  bool will_be_copied_on_migration(void *ptr) {
    return reinterpret_cast<uint8_t *>(ptr) > copy_start;
  }
};

class ProcletManager {
 public:
  ProcletManager();

  static void mmap(void *proclet_base);
  static void madvise_populate(void *proclet_base, uint64_t populate_len);
  static void munmap(void *proclet_base);
  static void allocate(void *proclet_base, bool migratable);
  static void deallocate(void *proclet_base);
  static void setup(void *proclet_base, bool migratable, bool from_migration);
  static void wait_until_present(ProcletHeader *proclet_header);
  void insert(void *proclet_base);
  bool remove_for_migration(void *proclet_base);
  bool remove_for_destruction(void *proclet_base);
  std::vector<void *> get_all_proclets();
  uint64_t get_mem_usage();
  uint32_t get_num_present_proclets();

 private:
  constexpr static uint32_t kNumAlwaysMmapedPages =
      (offsetof(ProcletHeader, always_mmaped_end) - 1) / kPageSize + 1;
  constexpr static uint32_t kNumAlwaysMmapedBytes =
      kNumAlwaysMmapedPages * kPageSize;

  std::vector<void *> present_proclets_;
  uint32_t num_present_proclets_;
  rt::Spin spin_;
  friend class MigrationEnabledGuard;
  friend class MigrationDisabledGuard;
  friend class NonBlockingMigrationDisabledGuard;
  friend class Test;

  bool try_disable_migration(ProcletHeader *proclet_header);
  void disable_migration(ProcletHeader *proclet_header);
  static void enable_migration(ProcletHeader *proclet_header);
};

class MigrationEnabledGuard {
 public:
  // By default guards the current proclet header.
  MigrationEnabledGuard();
  MigrationEnabledGuard(ProcletHeader *proclet_header);
  MigrationEnabledGuard(MigrationEnabledGuard &&o);
  MigrationEnabledGuard &operator=(MigrationEnabledGuard &&o);
  void reset(ProcletHeader *proclet_header = nullptr);
  ~MigrationEnabledGuard();

 private:
  ProcletHeader *proclet_header_;
};

class MigrationDisabledGuard {
 public:
  // By default guards the current proclet header.
  MigrationDisabledGuard();
  MigrationDisabledGuard(ProcletHeader *proclet_header);
  MigrationDisabledGuard(MigrationDisabledGuard &&o);
  MigrationDisabledGuard &operator=(MigrationDisabledGuard &&o);
  void reset(ProcletHeader *proclet_header = nullptr);
  ~MigrationDisabledGuard();
  operator bool() const;
  ProcletHeader *get_proclet_header();

 private:
  ProcletHeader *proclet_header_;
};

class NonBlockingMigrationDisabledGuard {
 public:
  // By default guards the current proclet header.
  NonBlockingMigrationDisabledGuard();
  NonBlockingMigrationDisabledGuard(ProcletHeader *proclet_header);
  NonBlockingMigrationDisabledGuard(NonBlockingMigrationDisabledGuard &&o);
  NonBlockingMigrationDisabledGuard &operator=(
      NonBlockingMigrationDisabledGuard &&o);
  ~NonBlockingMigrationDisabledGuard();
  void reset(ProcletHeader *proclet_header = nullptr);
  operator bool() const;
  ProcletHeader *get_proclet_header();

 private:
  ProcletHeader *proclet_header_;
};

}  // namespace nu

#include "nu/impl/proclet_mgr.ipp"

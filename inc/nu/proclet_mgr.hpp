#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <optional>
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
#include "nu/utils/spin_lock.hpp"
#include "nu/utils/time.hpp"

namespace nu {

enum ProcletStatus {
  kAbsent = 0,
  kPopulating,
  kDepopulating,
  kCleaning,
  kMigrating,
  kPresent,
  kDestructing,
};

// Proclet statuses are stored out of band so that they are always accessible
// even if the proclets are not present locally.
extern uint8_t proclet_statuses[kMaxNumProclets];
extern SpinLock proclet_migration_spin[kMaxNumProclets];

struct ProcletHeader {
  ~ProcletHeader() = default;

  // Used for monitoring cpu load.
  CPULoad cpu_load;

  // Max heap size.
  uint64_t populate_size;
  uint64_t capacity;

  // For synchronization.
  SpinLock spin_lock;
  CondVar cond_var;

  // Migration related.
  std::atomic<int8_t> pending_load_cnt;
  bool migratable;

  //--- Fields below will be automatically copied during migration. ---/
  uint8_t copy_start[0];

  // Used for monitoring active threads count.
  Counter thread_cnt;

  // Root object.
  void *root_obj;

  // Logical timer.
  Time time;

  // Record mutexes and condvars that have blocked waiters.
  BlockedSyncer blocked_syncer;

  // For disabling migration.
  RCULock rcu_lock;

  // Ref cnt related.
  int ref_cnt;

  // Heap mem allocator. Must be the last field.
  Counter slab_ref_cnt;
  SlabAllocator slab;

  uint64_t global_idx() const;
  uint64_t total_mem_size() const;
  uint64_t heap_size() const;
  uint64_t stack_size() const;
  uint8_t &status();
  uint8_t status() const;
  bool is_local() const;
  SpinLock &migration_spin();
  VAddrRange range() const;
};

class ProcletManager {
 public:
  ProcletManager();

  static void setup(void *proclet_base, uint64_t capacity, bool migratable,
                    bool from_migration);
  void cleanup(void *proclet_base, bool for_migration);
  static void madvise_populate(void *proclet_base, uint64_t populate_len);
  static void depopulate(void *proclet_base, uint64_t size, bool defer);
  static void wait_until_being_local(ProcletHeader *proclet_header);
  void insert(void *proclet_base);
  void undo_remove(void *proclet_base);
  bool remove_for_migration(void *proclet_base);
  bool remove_for_destruction(void *proclet_base);
  std::vector<void *> get_all_proclets();
  uint64_t get_mem_usage();
  uint32_t get_num_present_proclets();
  bool stash_timer_callback(TimerCallbackArg *arg);
  template <typename RetT>
  std::optional<RetT> get_proclet_info(
      const ProcletHeader *header,
      std::function<RetT(const ProcletHeader *)> f);

 private:
  std::vector<void *> present_proclets_;
  std::vector<TimerCallbackArg *> stashed_timer_cbs_;
  uint32_t num_present_proclets_;
  SpinLock spin_;
  friend class Test;

  bool __remove(void *proclet_base, ProcletStatus new_status);
};

}  // namespace nu

#include "nu/impl/proclet_mgr.ipp"

#include <sys/mman.h>

#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/rcu_hash_map.hpp"
#include "nu/utils/time.hpp"

namespace nu {

inline ProcletHeader::~ProcletHeader() {}

inline bool ProcletHeader::is_inside(void *ptr) {
  return (reinterpret_cast<uint64_t>(ptr) & (~(capacity - 1))) ==
         reinterpret_cast<uint64_t>(this);
}

inline uint64_t ProcletHeader::size() const {
  auto size_in_bytes = reinterpret_cast<uint64_t>(slab.get_base()) +
                       slab.get_usage() - reinterpret_cast<uint64_t>(this);
  return size_in_bytes;
}

inline uint8_t &ProcletHeader::status() {
  auto idx = (reinterpret_cast<uint64_t>(this) - kMinProcletHeapVAddr) /
             kMinProcletHeapSize;
  return proclet_statuses[idx];
}

inline RCULock &ProcletHeader::rcu_lock() {
  auto idx = (reinterpret_cast<uint64_t>(this) - kMinProcletHeapVAddr) /
             kMinProcletHeapSize;
  return proclet_rcu_locks[idx & (kMaxNumProcletRCULocks - 1)];
}

inline VAddrRange ProcletHeader::get_range() const {
  auto start_addr = reinterpret_cast<uint64_t>(this);
  auto end_addr = start_addr + capacity;
  return VAddrRange{start_addr, end_addr};
}

inline void ProcletManager::wait_until(ProcletHeader *proclet_header,
                                       ProcletStatus status) {
  proclet_header->spin_lock.lock();
  while (rt::access_once(proclet_header->status()) != status) {
    proclet_header->cond_var.wait(&proclet_header->spin_lock);
  }
  proclet_header->spin_lock.unlock();
}

inline void ProcletManager::insert(void *proclet_base) {
  rt::SpinGuard guard(&spin_);
  reinterpret_cast<ProcletHeader *>(proclet_base)->status() = kPresent;
  num_present_proclets_++;
  present_proclets_.push_back(proclet_base);
}

inline bool ProcletManager::remove_for_migration(void *proclet_base) {
  rt::SpinGuard guard(&spin_);
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  auto &status = proclet_header->status();
  if (status == kPresent) {
    num_present_proclets_--;
    status = kAbsent;
    return true;
  } else {
    return false;
  }
}

inline bool ProcletManager::remove_for_destruction(void *proclet_base) {
  rt::SpinGuard guard(&spin_);
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  auto &status = proclet_header->status();
  if (status == kPresent) {
    num_present_proclets_--;
    status = kDestructed;
    return true;
  } else {
    return false;
  }
}

inline void ProcletManager::enable_migration(ProcletHeader *proclet_header) {
  proclet_header->rcu_lock().reader_unlock();
}

inline bool ProcletManager::try_disable_migration(
    ProcletHeader *proclet_header) {
  auto &rcu_lock = proclet_header->rcu_lock();
  rcu_lock.reader_lock();
  if (unlikely(rt::access_once(proclet_header->status()) < kPresent)) {
    rcu_lock.reader_unlock();
    return false;
  }
  return true;
}

inline void ProcletManager::disable_migration(ProcletHeader *proclet_header) {
  auto &rcu_lock = proclet_header->rcu_lock();
  rcu_lock.reader_lock();
  if (unlikely(rt::access_once(proclet_header->status()) < kPresent)) {
    rcu_lock.reader_unlock();
    ProcletManager::wait_until(proclet_header, kPresent);
    rcu_lock.reader_lock();
  }
}

inline MigrationEnabledGuard::MigrationEnabledGuard()
    : MigrationEnabledGuard(Runtime::get_current_proclet_header()) {}

inline MigrationEnabledGuard::MigrationEnabledGuard(
    ProcletHeader *proclet_header)
    : proclet_header_(proclet_header) {
  if (proclet_header_) {
    ProcletManager::enable_migration(proclet_header_);
  }
}

inline MigrationEnabledGuard::MigrationEnabledGuard(MigrationEnabledGuard &&o)
    : proclet_header_(o.proclet_header_) {
  o.proclet_header_ = nullptr;
}

inline MigrationEnabledGuard &MigrationEnabledGuard::operator=(
    MigrationEnabledGuard &&o) {
  proclet_header_ = o.proclet_header_;
  o.proclet_header_ = nullptr;
  return *this;
}

inline MigrationEnabledGuard::~MigrationEnabledGuard() {
  if (proclet_header_) {
    Runtime::proclet_manager->disable_migration(proclet_header_);
  }
}

inline void MigrationEnabledGuard::reset(ProcletHeader *proclet_header) {
  this->~MigrationEnabledGuard();
  proclet_header_ = proclet_header;
  if (proclet_header) {
    Runtime::proclet_manager->enable_migration(proclet_header);
  }
}

inline MigrationDisabledGuard::MigrationDisabledGuard()
    : MigrationDisabledGuard(Runtime::get_current_proclet_header()) {}

inline MigrationDisabledGuard::MigrationDisabledGuard(
    ProcletHeader *proclet_header)
    : proclet_header_(proclet_header) {
  if (proclet_header_) {
    Runtime::proclet_manager->disable_migration(proclet_header_);
  }
}

inline MigrationDisabledGuard::MigrationDisabledGuard(
    MigrationDisabledGuard &&o)
    : proclet_header_(o.proclet_header_) {
  o.proclet_header_ = nullptr;
}

inline MigrationDisabledGuard &MigrationDisabledGuard::operator=(
    MigrationDisabledGuard &&o) {
  proclet_header_ = o.proclet_header_;
  o.proclet_header_ = nullptr;
  return *this;
}

inline MigrationDisabledGuard::~MigrationDisabledGuard() {
  if (proclet_header_) {
    ProcletManager::enable_migration(proclet_header_);
  }
}

inline MigrationDisabledGuard::operator bool() const { return proclet_header_; }

inline void MigrationDisabledGuard::reset(ProcletHeader *proclet_header) {
  this->~MigrationDisabledGuard();
  proclet_header_ = proclet_header;
  if (proclet_header) {
    Runtime::proclet_manager->disable_migration(proclet_header);
  }
}

inline ProcletHeader *MigrationDisabledGuard::get_proclet_header() {
  return proclet_header_;
}

inline NonBlockingMigrationDisabledGuard::NonBlockingMigrationDisabledGuard()
    : proclet_header_(Runtime::get_current_proclet_header()) {}

inline NonBlockingMigrationDisabledGuard::NonBlockingMigrationDisabledGuard(
    ProcletHeader *proclet_header)
    : proclet_header_(proclet_header) {
  if (proclet_header_) {
    if (unlikely(!Runtime::proclet_manager->try_disable_migration(
            proclet_header_))) {
      proclet_header_ = nullptr;
    }
  }
}

inline NonBlockingMigrationDisabledGuard::NonBlockingMigrationDisabledGuard(
    NonBlockingMigrationDisabledGuard &&o)
    : proclet_header_(o.proclet_header_) {
  o.proclet_header_ = nullptr;
}

inline NonBlockingMigrationDisabledGuard &
NonBlockingMigrationDisabledGuard::operator=(
    NonBlockingMigrationDisabledGuard &&o) {
  proclet_header_ = o.proclet_header_;
  o.proclet_header_ = nullptr;
  return *this;
}

inline NonBlockingMigrationDisabledGuard::~NonBlockingMigrationDisabledGuard() {
  if (proclet_header_) {
    ProcletManager::enable_migration(proclet_header_);
  }
}

inline NonBlockingMigrationDisabledGuard::operator bool() const {
  return proclet_header_;
}

inline void NonBlockingMigrationDisabledGuard::reset(
    ProcletHeader *proclet_header) {
  this->~NonBlockingMigrationDisabledGuard();
  proclet_header_ = proclet_header;
  if (proclet_header) {
    if (unlikely(
            !Runtime::proclet_manager->try_disable_migration(proclet_header))) {
      proclet_header_ = nullptr;
    }
  }
}

inline ProcletHeader *NonBlockingMigrationDisabledGuard::get_proclet_header() {
  return proclet_header_;
}

inline uint32_t ProcletManager::get_num_present_proclets() {
  return num_present_proclets_;
}

}  // namespace nu

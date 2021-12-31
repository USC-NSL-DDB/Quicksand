#include <sys/mman.h>

#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/rcu_hash_map.hpp"
#include "nu/utils/time.hpp"

namespace nu {

inline HeapHeader::~HeapHeader() {}

inline void HeapManager::wait_until_present(HeapHeader *heap_header) {
  heap_header->spin_lock.lock();
  while (rt::access_once(heap_header->status) < kPresent) {
    heap_header->cond_var.wait(&heap_header->spin_lock);
  }
  heap_header->spin_lock.unlock();
}

inline void HeapManager::insert(void *heap_base) {
  rt::SpinGuard guard(&spin_);
  reinterpret_cast<HeapHeader *>(heap_base)->status = kPresent;
  num_present_heaps_++;
  present_heaps_.push_back(heap_base);
}

inline bool HeapManager::remove_for_migration(void *heap_base) {
  rt::SpinGuard guard(&spin_);
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  if (heap_header->status == kPresent) {
    num_present_heaps_--;
    heap_header->status = kAbsent;
    return true;
  } else {
    return false;
  }
}

inline bool HeapManager::remove_for_destruction(void *heap_base) {
  rt::SpinGuard guard(&spin_);
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  if (heap_header->status == kPresent) {
    num_present_heaps_--;
    heap_header->status = kDestructed;
    return true;
  } else {
    return false;
  }
}

inline void HeapManager::enable_migration(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_unlock();
}

inline bool HeapManager::try_disable_migration(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_lock();
  if (unlikely(rt::access_once(heap_header->status) < kPresent)) {
    heap_header->rcu_lock.reader_unlock();
    return false;
  }
  return true;
}

inline void HeapManager::disable_migration(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_lock();
  if (unlikely(rt::access_once(heap_header->status) < kPresent)) {
    heap_header->rcu_lock.reader_unlock();
    HeapManager::wait_until_present(heap_header);
    heap_header->rcu_lock.reader_lock();
  }
}

inline MigrationEnabledGuard::MigrationEnabledGuard()
    : MigrationEnabledGuard(Runtime::get_current_obj_heap_header()) {}

inline MigrationEnabledGuard::MigrationEnabledGuard(HeapHeader *heap_header)
    : heap_header_(heap_header) {
  if (heap_header_) {
    HeapManager::enable_migration(heap_header_);
  }
}

inline MigrationEnabledGuard::MigrationEnabledGuard(MigrationEnabledGuard &&o)
    : heap_header_(o.heap_header_) {
  o.heap_header_ = nullptr;
}

inline MigrationEnabledGuard &
MigrationEnabledGuard::operator=(MigrationEnabledGuard &&o) {
  heap_header_ = o.heap_header_;
  o.heap_header_ = nullptr;
  return *this;
}

inline MigrationEnabledGuard::~MigrationEnabledGuard() {
  if (heap_header_) {
    Runtime::heap_manager->disable_migration(heap_header_);
  }
}

inline void MigrationEnabledGuard::reset(HeapHeader *heap_header) {
  this->~MigrationEnabledGuard();
  heap_header_ = heap_header;
  if (heap_header) {
    Runtime::heap_manager->enable_migration(heap_header);
  }
}

inline MigrationDisabledGuard::MigrationDisabledGuard()
    : MigrationDisabledGuard(Runtime::get_current_obj_heap_header()) {}

inline MigrationDisabledGuard::MigrationDisabledGuard(HeapHeader *heap_header)
    : heap_header_(heap_header) {
  if (heap_header_) {
    Runtime::heap_manager->disable_migration(heap_header_);
  }
}

inline MigrationDisabledGuard::MigrationDisabledGuard(
    MigrationDisabledGuard &&o)
    : heap_header_(o.heap_header_) {
  o.heap_header_ = nullptr;
}

inline MigrationDisabledGuard &
MigrationDisabledGuard::operator=(MigrationDisabledGuard &&o) {
  heap_header_ = o.heap_header_;
  o.heap_header_ = nullptr;
  return *this;
}

inline MigrationDisabledGuard::~MigrationDisabledGuard() {
  if (heap_header_) {
    HeapManager::enable_migration(heap_header_);
  }
}

inline MigrationDisabledGuard::operator bool() const { return heap_header_; }

inline void MigrationDisabledGuard::reset(HeapHeader *heap_header) {
  this->~MigrationDisabledGuard();
  heap_header_ = heap_header;
  if (heap_header) {
    Runtime::heap_manager->disable_migration(heap_header);
  }
}

inline HeapHeader *MigrationDisabledGuard::get_heap_header() {
  return heap_header_;
}

inline NonBlockingMigrationDisabledGuard::NonBlockingMigrationDisabledGuard()
    : heap_header_(Runtime::get_current_obj_heap_header()) {}

inline NonBlockingMigrationDisabledGuard::NonBlockingMigrationDisabledGuard(
    HeapHeader *heap_header)
    : heap_header_(heap_header) {
  if (heap_header_) {
    if (unlikely(!Runtime::heap_manager->try_disable_migration(heap_header_))) {
      heap_header_ = nullptr;
    }
  }
}

inline NonBlockingMigrationDisabledGuard::NonBlockingMigrationDisabledGuard(
    NonBlockingMigrationDisabledGuard &&o)
    : heap_header_(o.heap_header_) {
  o.heap_header_ = nullptr;
}

inline NonBlockingMigrationDisabledGuard &
NonBlockingMigrationDisabledGuard::operator=(
    NonBlockingMigrationDisabledGuard &&o) {
  heap_header_ = o.heap_header_;
  o.heap_header_ = nullptr;
  return *this;
}

inline NonBlockingMigrationDisabledGuard::~NonBlockingMigrationDisabledGuard() {
  if (heap_header_) {
    HeapManager::enable_migration(heap_header_);
  }
}

inline NonBlockingMigrationDisabledGuard::operator bool() const {
  return heap_header_;
}

inline void NonBlockingMigrationDisabledGuard::reset(HeapHeader *heap_header) {
  this->~NonBlockingMigrationDisabledGuard();
  heap_header_ = heap_header;
  if (heap_header) {
    if (unlikely(!Runtime::heap_manager->try_disable_migration(heap_header))) {
      heap_header_ = nullptr;
    }
  }
}

inline HeapHeader *NonBlockingMigrationDisabledGuard::get_heap_header() {
  return heap_header_;
}

inline uint32_t HeapManager::get_num_present_heaps() {
  return num_present_heaps_;
}

} // namespace nu

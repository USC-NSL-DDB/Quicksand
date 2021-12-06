#include <sys/mman.h>

#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/rcu_hash_map.hpp"
#include "nu/utils/time.hpp"

namespace nu {

inline HeapHeader::~HeapHeader() {}

inline void HeapManager::allocate(void *heap_base, bool migratable) {
  mmap(heap_base);
  setup(heap_base, migratable, /* from_migration = */ false);
}

inline void HeapManager::mmap(void *heap_base) {
  auto mmap_addr =
      ::mmap(reinterpret_cast<uint8_t *>(heap_base) + kNumAlwaysMmapedBytes,
             kHeapSize - kNumAlwaysMmapedBytes, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr == reinterpret_cast<void *>(-1));
}

inline void HeapManager::insert(void *heap_base) {
  rt::SpinGuard guard(&spin_);
  reinterpret_cast<HeapHeader *>(heap_base)->present = true;
  BUG_ON(!present_heaps_.emplace(heap_base).second);
}

inline bool HeapManager::remove_with_present(void *heap_base) {
  rt::SpinGuard guard(&spin_);
  return present_heaps_.erase(heap_base);
}

inline bool HeapManager::remove(void *heap_base) {
  rt::SpinGuard guard(&spin_);
  reinterpret_cast<HeapHeader *>(heap_base)->present = false;
  return present_heaps_.erase(heap_base);
}

inline void HeapManager::mark_absent(void *heap_base) {
  reinterpret_cast<HeapHeader *>(heap_base)->present = false;
}

inline void HeapManager::enable_migration(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_unlock();
}

inline bool HeapManager::try_disable_migration(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_lock();
  if (unlikely(!rt::access_once(heap_header->present))) {
    heap_header->rcu_lock.reader_unlock();
    return false;
  }
  return true;
}

inline void HeapManager::disable_migration(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_lock();
  if (unlikely(!rt::access_once(heap_header->present))) {
    heap_header->rcu_lock.reader_unlock();
    heap_header->mutex.lock();
    while (!rt::access_once(heap_header->present)) {
      heap_header->cond_var.wait(&heap_header->mutex);
    }
    heap_header->mutex.unlock();
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

inline void MigrationEnabledGuard::reset() {
  this->~MigrationEnabledGuard();
  heap_header_ = nullptr;
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

inline void MigrationDisabledGuard::reset() {
  this->~MigrationDisabledGuard();
  heap_header_ = nullptr;
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

inline void NonBlockingMigrationDisabledGuard::reset() {
  this->~NonBlockingMigrationDisabledGuard();
  heap_header_ = nullptr;
}

inline HeapHeader *NonBlockingMigrationDisabledGuard::get_heap_header() {
  return heap_header_;
}

inline uint32_t HeapManager::get_num_present_heaps() {
  rt::SpinGuard guard(&spin_);
  return present_heaps_.size();
}

} // namespace nu

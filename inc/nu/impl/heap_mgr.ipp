#include <sys/mman.h>

#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/time.hpp"
#include "nu/utils/rcu_hash_map.hpp"

namespace nu {

inline HeapHeader::~HeapHeader() {}

inline HeapManager::HeapManager()
    : active_heaps_(new decltype(active_heaps_)::element_type()) {}

inline void HeapManager::allocate(void *heap_base, bool migratable) {
  mmap(heap_base);
  setup(heap_base, migratable, /* from_migration = */ false);
}

inline void HeapManager::mmap(void *heap_base) {
  auto mmap_addr = ::mmap(heap_base, kHeapSize, PROT_READ | PROT_WRITE,
                          MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != heap_base);
}

inline void HeapManager::deallocate(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  heap_header->~HeapHeader();
  BUG_ON(munmap(heap_base, kHeapSize) == -1);
}

inline void HeapManager::insert(void *heap_base) {
  active_heaps_->put(reinterpret_cast<HeapHeader *>(heap_base), false);
}

inline bool HeapManager::contains(void *heap_base) {
  return active_heaps_->get(reinterpret_cast<HeapHeader *>(heap_base));
}

inline bool HeapManager::remove(void *heap_base) {
  return active_heaps_->remove(reinterpret_cast<HeapHeader *>(heap_base));
}

inline void HeapManager::migration_enable_final(HeapHeader *heap_header) {
  heap_header->rcu_lock.reader_unlock();
}

inline void HeapManager::migration_enable(HeapHeader *heap_header) {
  heap_header->threads->put(thread_self());
  heap_header->rcu_lock.reader_unlock();
}

inline MigrationEnabledGuard::MigrationEnabledGuard()
    : MigrationEnabledGuard(Runtime::get_current_obj_heap_header()) {}

inline MigrationEnabledGuard::MigrationEnabledGuard(HeapHeader *heap_header)
    : heap_header_(heap_header) {
  if (heap_header_) {
    HeapManager::migration_enable(heap_header_);
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
    Runtime::heap_manager->migration_disable(heap_header_);
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
    Runtime::heap_manager->migration_disable(heap_header_);
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
    HeapManager::migration_enable(heap_header_);
  }
}

inline MigrationDisabledGuard::operator bool() const { return heap_header_; }

inline void MigrationDisabledGuard::reset() {
  this->~MigrationDisabledGuard();
  heap_header_ = nullptr;
}

inline OutermostMigrationDisabledGuard::OutermostMigrationDisabledGuard(
    HeapHeader *heap_header)
    : heap_header_(heap_header) {
  if (unlikely(
          !Runtime::heap_manager->migration_disable_initial(heap_header_))) {
    heap_header_ = nullptr;
  }
}

inline OutermostMigrationDisabledGuard::OutermostMigrationDisabledGuard(
    OutermostMigrationDisabledGuard &&o)
    : heap_header_(o.heap_header_) {
  o.heap_header_ = nullptr;
}

inline OutermostMigrationDisabledGuard &
OutermostMigrationDisabledGuard::operator=(
    OutermostMigrationDisabledGuard &&o) {
  heap_header_ = o.heap_header_;
  o.heap_header_ = nullptr;
  return *this;
}

inline OutermostMigrationDisabledGuard::~OutermostMigrationDisabledGuard() {
  if (heap_header_) {
    HeapManager::migration_enable_final(heap_header_);
  }
}

inline OutermostMigrationDisabledGuard::operator bool() const {
  return heap_header_;
}

inline void OutermostMigrationDisabledGuard::reset() {
  this->~OutermostMigrationDisabledGuard();
  heap_header_ = nullptr;
}

} // namespace nu

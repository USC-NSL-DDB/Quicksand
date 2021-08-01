#include <sys/mman.h>

#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/time.hpp"
#include "nu/utils/rcu_hash_map.hpp"

namespace nu {

inline HeapHeader::~HeapHeader() {}

inline HeapManager::HeapManager()
    : heap_statuses_(new decltype(heap_statuses_)::element_type()) {}

inline void HeapManager::deallocate(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  heap_header->~HeapHeader();
  BUG_ON(munmap(heap_base, kHeapSize) == -1);
}

inline void HeapManager::insert(void *heap_base) {
  heap_statuses_->put(reinterpret_cast<HeapHeader *>(heap_base), PRESENT);
}

inline bool HeapManager::is_present(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  auto *v = heap_statuses_->get(heap_header);
  return v && *v == PRESENT;
}

inline bool HeapManager::remove(void *heap_base) {
  return heap_statuses_->remove(reinterpret_cast<HeapHeader *>(heap_base));
}

inline bool HeapManager::remove_if_not_migrating(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  return heap_statuses_->remove_if_equals(heap_header, PRESENT);
}

inline bool HeapManager::mark_migrating(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  if (!heap_statuses_->update_if_equals(heap_header, PRESENT, MIGRATING)) {
    return false;
  }
  ACCESS_ONCE(heap_header->migrating) = true;
  return true;
}

inline HeapStatus *HeapManager::get_status(void *heap_base) {
  return heap_statuses_->get(reinterpret_cast<HeapHeader *>(heap_base));
}

inline void HeapManager::migration_enable_final(HeapHeader *heap_header) {
  heap_header->threads->remove(thread_self());
  heap_header->rcu_lock.reader_unlock();
}

inline void HeapManager::migration_disable() {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (!heap_header) {
    return;
  }
  heap_header->rcu_lock.reader_lock();
}

inline void HeapManager::migration_enable() {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (!heap_header) {
    return;
  }
  heap_header->rcu_lock.reader_unlock();
}

} // namespace nu

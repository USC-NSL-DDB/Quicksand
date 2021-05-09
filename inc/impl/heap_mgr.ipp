#include <sys/mman.h>

#include "runtime_alloc.hpp"
#include "time.hpp"

namespace nu {

inline HeapHeader::~HeapHeader() {}

inline HeapManager::HeapManager()
    : heap_statuses_(new decltype(heap_statuses_)::element_type()) {}

inline void HeapManager::deallocate(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  heap_header->~HeapHeader();
  BUG_ON(munmap(heap_base, kHeapSize) == -1);
}

inline SlabAllocator *HeapManager::get_slab(void *heap_base) {
  return &(reinterpret_cast<HeapHeader *>(heap_base)->slab);
}

inline void HeapManager::insert(void *heap_base) {
  heap_statuses_->put(reinterpret_cast<HeapHeader *>(heap_base));
}

inline bool HeapManager::contains(void *heap_base) {
  return heap_statuses_->contains(reinterpret_cast<HeapHeader *>(heap_base));
}

inline bool HeapManager::remove(void *heap_base) {
  return heap_statuses_->remove(reinterpret_cast<HeapHeader *>(heap_base));
}

inline void HeapManager::rcu_reader_lock() { rcu_lock_.reader_lock(); }

inline bool HeapManager::rcu_reader_lock_np() {
  return rcu_lock_.reader_lock_np();
}

inline void HeapManager::rcu_reader_unlock() { rcu_lock_.reader_unlock(); }

inline void HeapManager::rcu_reader_unlock_np() {
  rcu_lock_.reader_unlock_np();
}

inline void HeapManager::rcu_writer_sync() { rcu_lock_.writer_sync(); }

} // namespace nu

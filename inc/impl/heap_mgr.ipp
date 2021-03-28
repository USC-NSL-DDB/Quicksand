#include <sys/mman.h>

#include "runtime_alloc.hpp"

namespace nu {

inline HeapManager::HeapManager()
    : heap_statuses_(new decltype(heap_statuses_)::element_type()) {}

inline void HeapManager::deallocate(void *heap_base) {
  BUG_ON(munmap(heap_base, kHeapSize) == -1);
}

inline SlabAllocator *HeapManager::get_slab(void *heap_base) {
  return &(reinterpret_cast<HeapHeader *>(heap_base)->slab);
}

inline void HeapManager::insert(void *heap_base) {
  heap_statuses_->put(heap_base);
}

inline bool HeapManager::contains(void *heap_base) {
  return heap_statuses_->contains(heap_base);
}

inline bool HeapManager::remove(void *heap_base) {
  return heap_statuses_->remove(heap_base);
}

inline void HeapManager::rcu_lock() { rcu_lock_.lock(); }

inline void HeapManager::rcu_unlock() { rcu_lock_.unlock(); }

inline void HeapManager::rcu_synchronize() { rcu_lock_.synchronize(); }

} // namespace nu

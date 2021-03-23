#include <utility>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "runtime_alloc.hpp"
#include "heap_mgr.hpp"

namespace nu {

template <typename T> T *Runtime::setup_thread_env(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  heap_header->rcu_lock.lock();
  if (heap_header->migrating) {
    heap_header->rcu_lock.unlock();
    return nullptr;
  }
  heap_header->threads->put(thread_self());
  heap_header->rcu_lock.unlock();
  return switch_to_obj_heap<T>(heap_base);
}

inline void Runtime::clear_thread_env(void *heap_base) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  heap_header->threads->remove(thread_self());
  switch_to_runtime_heap();
}

template <typename T> T *Runtime::switch_to_obj_heap(void *heap_base) {
  auto *slab = heap_manager->get_slab(heap_base);
  set_uthread_specific(reinterpret_cast<uint64_t>(slab));
  auto *obj_ptr = reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(slab->get_base()) + sizeof(PtrHeader));
  return obj_ptr;
}

inline void Runtime::switch_to_runtime_heap() { set_uthread_specific(0); }

template <typename T, typename... Args>
T *Runtime::new_on_runtime_heap(Args &&... args) {
  auto ptr = Runtime::runtime_slab.allocate(sizeof(T));
  new (ptr) T(std::forward<Args>(args)...);
  return reinterpret_cast<T *>(ptr);
}

template <typename T> void Runtime::delete_on_runtime_heap(T *ptr) {
  ptr->~T();
  Runtime::runtime_slab.free(ptr);
}

} // namespace nu

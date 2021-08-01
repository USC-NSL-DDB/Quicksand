#include <utility>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <base/stddef.h>
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "nu/commons.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/stack_manager.hpp"

namespace nu {

inline void *Runtime::get_heap() {
  return reinterpret_cast<void *>(get_uthread_specific());
}

inline void Runtime::set_heap(void *heap) {
  return set_uthread_specific(reinterpret_cast<uint64_t>(heap));
}

inline void Runtime::switch_to_obj_heap(void *obj_ptr) {
  auto slab_base = reinterpret_cast<uint64_t>(obj_ptr);
  auto *heap_header = reinterpret_cast<HeapHeader *>(slab_base) - 1;
  set_heap(&heap_header->slab);
}

inline void Runtime::switch_to_runtime_heap() { set_heap(nullptr); }

inline HeapHeader *Runtime::get_current_obj_heap_header() {
  auto obj_slab = reinterpret_cast<nu::SlabAllocator *>(get_heap());
  if (!obj_slab) {
    return nullptr;
  }
  auto *heap_header = container_of(obj_slab, HeapHeader, slab);
  return heap_header;
}

inline RemObjID Runtime::get_current_obj_id() {
  auto *heap_base = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_base);
  return to_obj_id(heap_base);
}

template <typename T> T *Runtime::get_current_obj() {
  auto obj_slab = reinterpret_cast<nu::SlabAllocator *>(get_heap());
  if (!obj_slab) {
    return nullptr;
  }
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(obj_slab->get_base()));
}

template <typename T> T *Runtime::get_obj(RemObjID id) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(to_heap_base(id));
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(heap_header->slab.get_base()));
}

template <typename Cls, typename Fn, typename... As>
void __attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer")))
Runtime::__run_within_obj_env(HeapHeader *heap_header, uint8_t *obj_stack,
                              Cls *obj_ptr, Fn fn, As &&... args) {
  fn(*obj_ptr, std::forward<As>(args)...);

  if (unlikely(thread_is_migrated())) {
    auto runtime_stack_base = thread_get_runtime_stack_base();
    switch_to_runtime_stack(runtime_stack_base);
    heap_manager->migration_enable_final(heap_header);
    rt::Exit();
  }
}

// By default, fn will be invoked with migration disabled.
template <typename Cls, typename Fn, typename... As>
bool __attribute__((optimize("no-omit-frame-pointer")))
Runtime::run_within_obj_env(void *heap_base, Fn fn, As &&... args) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);

  if (unlikely(!heap_manager->migration_disable_initial(heap_header))) {
    return false;
  }
  auto &slab = heap_header->slab;
  auto *obj_ptr =
      reinterpret_cast<Cls *>(reinterpret_cast<uintptr_t>(slab.get_base()));
  switch_to_obj_heap(obj_ptr);

  auto *obj_stack = Runtime::stack_manager->get();
  BUG_ON(reinterpret_cast<uintptr_t>(obj_stack) % kStackAlignment);
  auto *old_rsp = switch_to_obj_stack(obj_stack);

  __run_within_obj_env<Cls>(heap_header, obj_stack, obj_ptr, fn,
                            std::forward<As>(args)...);

  switch_to_runtime_stack(old_rsp);
  Runtime::stack_manager->put(obj_stack);
  switch_to_runtime_heap();
  heap_manager->migration_enable_final(heap_header);

  return true;
}

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

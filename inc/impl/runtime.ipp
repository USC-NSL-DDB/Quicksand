#include <utility>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <base/stddef.h>
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "defs.hpp"
#include "heap_mgr.hpp"
#include "runtime_alloc.hpp"

namespace nu {

inline __attribute__((always_inline)) uint64_t
switch_to_obj_stack(uint64_t stack) {
  uint64_t old_rsp;
  asm volatile("movq %%rsp, %0\n\t"
               "movq %1, %%rsp"
               : "=&r"(old_rsp)
               : "r"(stack)
               :);
  thread_set_obj_stack(reinterpret_cast<void *>(stack));
  return old_rsp;
}

inline __attribute__((always_inline)) void
switch_to_runtime_stack(uint64_t old_rsp) {
  asm volatile("movq %0, %%rsp" : : "r"(old_rsp) :);
  thread_unset_obj_stack();
}

inline void Runtime::switch_to_obj_heap(void *obj_ptr) {
  auto slab_base = reinterpret_cast<uint64_t>(obj_ptr) - sizeof(PtrHeader);
  auto *heap_header = reinterpret_cast<HeapHeader *>(slab_base) - 1;
  set_uthread_specific(reinterpret_cast<uint64_t>(&heap_header->slab));
}

inline void Runtime::switch_to_runtime_heap() { set_uthread_specific(0); }

inline HeapHeader *Runtime::get_obj_heap_header() {
  auto obj_slab = reinterpret_cast<nu::SlabAllocator *>(get_uthread_specific());
  if (!obj_slab) {
    return nullptr;
  }
  auto *heap_header = container_of(obj_slab, HeapHeader, slab);
  return heap_header;
}

template <typename Cls, typename Fn, typename... As>
void __attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer")))
Runtime::__run_within_obj_env(SlabAllocator *slab, uint64_t obj_stack_base,
                              Cls *obj_ptr, Fn fn, As &&... args) {
  fn(*obj_ptr, std::forward<As>(args)...);

  if (unlikely(thread_is_migrated())) {
    auto runtime_stack_base = thread_get_runtime_stack_base();
    switch_to_runtime_stack(runtime_stack_base);
    slab->free(reinterpret_cast<void *>(obj_stack_base));
    heap_manager->rcu_reader_unlock();
    rt::Exit();
  }
}

// By default, fn will be invoked with migration disabled.
template <typename Cls, typename Fn, typename... As>
bool __attribute__((optimize("no-omit-frame-pointer")))
Runtime::run_within_obj_env(void *heap_base, Fn fn, As &&... args) {
  heap_manager->rcu_reader_lock();
  if (unlikely(!heap_manager->contains(heap_base))) {
    heap_manager->rcu_reader_unlock();
    return false;
  }
  auto *slab = heap_manager->get_slab(heap_base);
  auto *obj_ptr = reinterpret_cast<Cls *>(
      reinterpret_cast<uintptr_t>(slab->get_base()) + sizeof(PtrHeader));
  switch_to_obj_heap(obj_ptr);

  auto obj_stack_base =
      reinterpret_cast<uint64_t>(slab->allocate(kStackSize + kStackAlignment));
  auto aligned_obj_top = obj_stack_base + kStackSize + kStackAlignment - 1;
  aligned_obj_top &= ~(kStackAlignment - 1);
  auto old_rsp = switch_to_obj_stack(aligned_obj_top);

  __run_within_obj_env<Cls>(slab, obj_stack_base, obj_ptr, fn,
                            std::forward<As>(args)...);

  switch_to_runtime_stack(old_rsp);
  slab->free(reinterpret_cast<void *>(obj_stack_base));
  switch_to_runtime_heap();
  heap_manager->rcu_reader_unlock();

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

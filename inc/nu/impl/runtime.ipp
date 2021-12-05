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

inline void *Runtime::switch_slab(void *slab) {
  return thread_set_obj_slab(slab);
}

inline void *Runtime::switch_to_runtime_slab() {
  return thread_set_obj_slab(nullptr);
}

inline HeapHeader *Runtime::get_current_obj_heap_header() {
  auto obj_slab = reinterpret_cast<nu::SlabAllocator *>(thread_get_obj_slab());
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
  auto obj_slab = reinterpret_cast<nu::SlabAllocator *>(thread_get_obj_slab());
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

template <typename Cls, typename... A0s, typename... A1s>
__attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer"))) void
__run_within_obj_env(OutermostMigrationDisabledGuard *guard, uint8_t *obj_stack,
                     Cls *obj_ptr, void (*fn)(A0s...), A1s &&... args) {
  OutermostMigrationDisabledGuard guard_on_obj_stack(std::move(*guard));
  auto *heap_header = guard_on_obj_stack.get_heap_header();

  fn(*obj_ptr, std::forward<A1s>(args)...);

  if (unlikely(thread_is_migrated())) {
    heap_header->migrated_wg.Done();
    auto runtime_stack_base = thread_get_runtime_stack_base();
    switch_stack(runtime_stack_base);
    rt::Exit();
  }
}

// By default, fn will be invoked with migration disabled.
template <typename Cls, typename... A0s, typename... A1s>
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::run_within_obj_env(void *heap_base, void (*fn)(A0s...),
                            A1s &&... args) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  OutermostMigrationDisabledGuard guard(heap_header);
  if (unlikely(!guard)) {
    return false;
  }

  thread_set_creator_ip_and_owner_heap(get_cfg_ip(), heap_base);

  auto *obj_stack = Runtime::stack_manager->get();
  assert(reinterpret_cast<uintptr_t>(obj_stack) % kStackAlignment == 0);
  auto &slab = heap_header->slab;

  switch_slab(&slab);
  auto *old_rsp = switch_stack(obj_stack);

  auto *obj_ptr =
      reinterpret_cast<Cls *>(reinterpret_cast<uintptr_t>(slab.get_base()));
  __run_within_obj_env<Cls>(&guard, obj_stack, obj_ptr, fn,
                            std::forward<A1s>(args)...);

  switch_stack(old_rsp);
  switch_to_runtime_slab();
  Runtime::stack_manager->put(obj_stack);
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

inline RuntimeSlabGuard::RuntimeSlabGuard() {
  original_slab_ = Runtime::switch_to_runtime_slab();
}

inline RuntimeSlabGuard::~RuntimeSlabGuard() {
  thread_set_obj_slab(original_slab_);
}

inline ObjSlabGuard::ObjSlabGuard(void *slab) {
  original_slab_ = Runtime::switch_slab(slab);
}

inline ObjSlabGuard::~ObjSlabGuard() { thread_set_obj_slab(original_slab_); }

} // namespace nu

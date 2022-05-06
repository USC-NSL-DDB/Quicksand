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

inline SlabAllocator *Runtime::get_current_obj_slab() {
  return reinterpret_cast<nu::SlabAllocator *>(thread_get_obj_slab());
}

inline HeapHeader *Runtime::get_current_obj_heap_header() {
  return reinterpret_cast<HeapHeader *>(thread_get_owner_heap());
}

inline ProcletID Runtime::get_current_obj_id() {
  auto *heap_base = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_base);
  return to_obj_id(heap_base);
}

template <typename T> T *Runtime::get_current_obj() {
  auto *heap_header = get_current_obj_heap_header();
  if (!heap_header) {
    return nullptr;
  }
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(heap_header->slab.get_base()));
}

template <typename T> T *Runtime::get_obj(ProcletID id) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(to_heap_base(id));
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(heap_header->slab.get_base()));
}

template <typename Cls, typename... A0s, typename... A1s>
__attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer"))) void
__run_within_obj_env(NonBlockingMigrationDisabledGuard *guard, Cls *obj_ptr,
                     void (*fn)(A0s...), A1s &&... args) {
  NonBlockingMigrationDisabledGuard guard_on_obj_stack(std::move(*guard));
  // auto *heap_header = guard_on_obj_stack.get_heap_header();

  fn(*obj_ptr, std::forward<A1s>(args)...);

  thread_unset_owner_heap();
  if (unlikely(thread_has_been_migrated())) {
    // FIXME
    // heap_header->migrated_wg.Done();
    switch_stack(thread_get_runtime_stack_base());
    rt::Exit();
  }
}

// By default, fn will be invoked with migration disabled.
template <typename Cls, typename... A0s, typename... A1s>
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::run_within_obj_env(void *heap_base, void (*fn)(A0s...),
                            A1s &&... args) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
retry:
  NonBlockingMigrationDisabledGuard guard(heap_header);
  if (unlikely(!guard)) {
    if (unlikely(rt::access_once(heap_header->status) >= kMapped)) {
      HeapManager::wait_until_present(heap_header);
      goto retry;
    } else {
      return false;
    }
  }

  auto *slab = &heap_header->slab;
  auto *obj_ptr =
      reinterpret_cast<Cls *>(reinterpret_cast<uintptr_t>(slab->get_base()));
  auto *obj_stack = Runtime::stack_manager->get();
  assert(reinterpret_cast<uintptr_t>(obj_stack) % kStackAlignment == 0);

  switch_slab(slab);
  thread_set_owner_heap(thread_self(), heap_base);
  auto *old_rsp = switch_stack(obj_stack);

  __run_within_obj_env<Cls>(&guard, obj_ptr, fn, std::forward<A1s>(args)...);

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

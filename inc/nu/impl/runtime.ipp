#include <utility>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <base/stddef.h>
#include <net/ip.h>
#include <runtime/tcp.h>
}

#include "nu/commons.hpp"
#include "nu/proclet_mgr.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/stack_manager.hpp"

namespace nu {

inline void *Runtime::switch_slab(void *slab) {
  return thread_set_proclet_slab(slab);
}

inline void *Runtime::switch_to_runtime_slab() {
  return thread_set_proclet_slab(nullptr);
}

inline SlabAllocator *Runtime::get_current_proclet_slab() {
  return reinterpret_cast<nu::SlabAllocator *>(thread_get_proclet_slab());
}

inline ProcletHeader *Runtime::get_current_proclet_header() {
  return reinterpret_cast<ProcletHeader *>(thread_get_owner_proclet());
}

template <typename T>
T *Runtime::get_current_root_obj() {
  auto *proclet_header = get_current_proclet_header();
  if (!proclet_header) {
    return nullptr;
  }
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(proclet_header->slab.get_base()));
}

template <typename T>
T *Runtime::get_root_obj(ProcletID id) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(to_proclet_base(id));
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(proclet_header->slab.get_base()));
}

template <typename Cls, typename... A0s, typename... A1s>
__attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer"))) void
__run_within_proclet_env(NonBlockingMigrationDisabledGuard *guard, Cls *obj_ptr,
                         void (*fn)(A0s...), A1s &&... args) {
  {
    NonBlockingMigrationDisabledGuard guard_on_proclet_stack(std::move(*guard));
    // auto *proclet_header = guard_on_proclet_stack.get_proclet_header();

    fn(*obj_ptr, std::forward<A1s>(args)...);
  }

  thread_unset_owner_proclet();
  if (unlikely(thread_has_been_migrated())) {
    // FIXME
    // proclet_header->migrated_wg.Done();
    switch_stack(thread_get_runtime_stack_base());
    rt::Exit();
  }
}

// By default, fn will be invoked with migration disabled.
template <typename Cls, typename... A0s, typename... A1s>
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                A1s &&... args) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);

  NonBlockingMigrationDisabledGuard guard(proclet_header);
  if (unlikely(!guard)) {
    if (unlikely(rt::access_once(proclet_header->status()) != kAbsent)) {
      ProcletManager::wait_until(proclet_header, kAbsent);
    }
    return false;
  }

  auto *slab = &proclet_header->slab;
  auto *obj_ptr =
      reinterpret_cast<Cls *>(reinterpret_cast<uintptr_t>(slab->get_base()));
  auto *proclet_stack = Runtime::stack_manager->get();
  assert(reinterpret_cast<uintptr_t>(proclet_stack) % kStackAlignment == 0);

  switch_slab(slab);
  thread_set_owner_proclet(thread_self(), proclet_base);
  auto *old_rsp = switch_stack(proclet_stack);

  __run_within_proclet_env<Cls>(&guard, obj_ptr, fn,
                                std::forward<A1s>(args)...);

  switch_stack(old_rsp);
  switch_to_runtime_slab();
  Runtime::stack_manager->put(proclet_stack);
  return true;
}

template <typename T, typename... Args>
T *Runtime::new_on_runtime_heap(Args &&... args) {
  auto ptr = Runtime::runtime_slab.allocate(sizeof(T));
  new (ptr) T(std::forward<Args>(args)...);
  return reinterpret_cast<T *>(ptr);
}

template <typename T>
void Runtime::delete_on_runtime_heap(T *ptr) {
  ptr->~T();
  Runtime::runtime_slab.free(ptr);
}

inline RuntimeSlabGuard::RuntimeSlabGuard() {
  original_slab_ = Runtime::switch_to_runtime_slab();
}

inline RuntimeSlabGuard::~RuntimeSlabGuard() {
  thread_set_proclet_slab(original_slab_);
}

inline ProcletSlabGuard::ProcletSlabGuard(void *slab) {
  original_slab_ = Runtime::switch_slab(slab);
}

inline ProcletSlabGuard::~ProcletSlabGuard() {
  thread_set_proclet_slab(original_slab_);
}

}  // namespace nu

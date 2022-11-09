#include <experimental/scope>
#include <type_traits>
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

[[gnu::always_inline]] inline void *Runtime::switch_stack(void *new_rsp) {
  assert(reinterpret_cast<uintptr_t>(new_rsp) % kStackAlignment == 0);
  void *old_rsp;
  asm volatile(
      "movq %%rsp, %0\n\t"
      "movq %1, %%rsp"
      : "=&r"(old_rsp)
      : "r"(new_rsp)
      :);
  return old_rsp;
}

[[gnu::always_inline]] inline void Runtime::switch_to_runtime_stack() {
  auto *runtime_rsp = thread_get_runtime_stack_base();
  switch_stack(runtime_rsp);
}

inline VAddrRange Runtime::get_proclet_stack_range(thread_t *thread) {
  VAddrRange range;
  auto rsp = thread_get_rsp(thread);
  range.start = rsp - kStackRedZoneSize;
  range.end = ((rsp + kStackSize) & (~(kStackSize - 1)));
  return range;
}

inline SlabAllocator *Runtime::get_current_proclet_slab() {
  return reinterpret_cast<nu::SlabAllocator *>(thread_get_proclet_slab());
}

inline ProcletHeader *Runtime::get_current_proclet_header() {
  return reinterpret_cast<ProcletHeader *>(thread_get_owner_proclet());
}

template <typename T>
inline T *Runtime::get_current_root_obj() {
  auto *proclet_header = get_current_proclet_header();
  if (!proclet_header) {
    return nullptr;
  }
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(proclet_header->slab.get_base()));
}

template <typename T>
inline T *Runtime::get_root_obj(ProcletID id) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(to_proclet_base(id));
  return reinterpret_cast<T *>(
      reinterpret_cast<uintptr_t>(proclet_header->slab.get_base()));
}

template <typename Cls, typename... A0s, typename... A1s>
__attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::__run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                  A1s &&... args) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  auto optional_migration_guard = attach_and_disable_migration(proclet_header);
  if (unlikely(!optional_migration_guard)) {
    return false;
  }
  auto &migration_guard = *optional_migration_guard;

  auto *obj_ptr = get_current_root_obj<Cls>();
  fn(&migration_guard, obj_ptr, std::forward<A1s>(args)...);
  detach(migration_guard);

  if (unlikely(thread_has_been_migrated())) {
    migration_guard.reset();
    auto proclet_stack_base = get_proclet_stack_range(__self).end;
    switch_stack(thread_get_runtime_stack_base());
    Runtime::stack_manager->put(
        reinterpret_cast<uint8_t *>(proclet_stack_base));
    rt::Exit();
  }

  return true;
}

// By default, fn will be invoked with preemption disabled.
template <typename Cls, typename... A0s, typename... A1s>
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                A1s &&... args) {
  bool ret;
  auto *proclet_stack = Runtime::stack_manager->get();
  assert(reinterpret_cast<uintptr_t>(proclet_stack) % kStackAlignment == 0);
  auto *old_rsp = switch_stack(proclet_stack);
  ret = __run_within_proclet_env<Cls>(proclet_base, fn,
                                      std::forward<A1s>(args)...);
  switch_stack(old_rsp);
  Runtime::stack_manager->put(proclet_stack);
  return ret;
}

template <typename T, typename... Args>
inline T *Runtime::new_on_runtime_heap(Args &&... args) {
  auto ptr = Runtime::runtime_slab.allocate(sizeof(T));
  new (ptr) T(std::forward<Args>(args)...);
  return reinterpret_cast<T *>(ptr);
}

template <typename T>
inline void Runtime::delete_on_runtime_heap(T *ptr) {
  ptr->~T();
  Runtime::runtime_slab.free(ptr);
}

template <typename T>
inline WeakProclet<T> Runtime::get_current_weak_proclet() {
  return WeakProclet<T>(to_proclet_id(get_current_proclet_header()));
}

inline void Runtime::detach(const MigrationGuard &g) {
  thread_unset_owner_proclet();
}

inline std::optional<MigrationGuard> Runtime::__reattach_and_disable_migration(
    ProcletHeader *new_header) {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  auto *old_header = thread_set_owner_proclet(thread_self(), new_header, true);
  if (!new_header) {
    return MigrationGuard(nullptr);
  } else if (new_header->status() != kAbsent) {
    auto nesting_cnt = new_header->rcu_lock.reader_lock();
    if (likely(new_header->status() != kAbsent || nesting_cnt > 1)) {
      return MigrationGuard(new_header);
    }
    new_header->rcu_lock.reader_unlock();
  }

  thread_set_owner_proclet(thread_self(), old_header, false);
  return std::nullopt;
}

inline std::optional<MigrationGuard> Runtime::attach_and_disable_migration(
    ProcletHeader *proclet_header) {
  assert(!thread_get_owner_proclet());
  return __reattach_and_disable_migration(proclet_header);
}

inline std::optional<MigrationGuard> Runtime::reattach_and_disable_migration(
    ProcletHeader *new_header, const MigrationGuard &old_guard) {
  assert(thread_get_owner_proclet() == old_guard.header());
  return __reattach_and_disable_migration(new_header);
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

inline MigrationGuard::MigrationGuard() {
  header_ = Runtime::get_current_proclet_header();
  if (header_) {
  retry:
    auto nesting_cnt = header_->rcu_lock.reader_lock();
    if (unlikely(header_->status() == kAbsent && nesting_cnt == 1)) {
      header_->rcu_lock.reader_unlock();
      ProcletManager::wait_until(header_, kPresent);
      goto retry;
    }
  }
}

inline MigrationGuard::MigrationGuard(ProcletHeader *header)
    : header_(header) {}

inline MigrationGuard::MigrationGuard(MigrationGuard &&o) : header_(o.header_) {
  o.header_ = nullptr;
}

inline MigrationGuard::~MigrationGuard() {
  if (header_) {
    header_->rcu_lock.reader_unlock();
  }
}

inline ProcletHeader *MigrationGuard::header() const { return header_; }

template <typename F>
inline auto MigrationGuard::enable_for(F &&f) {
  using RetT = decltype(f());

  auto cleaner = std::experimental::scope_exit([&] {
    retry:
      auto nesting_cnt = header_->rcu_lock.reader_lock();
      if (unlikely(header_->status() == kAbsent && nesting_cnt == 1)) {
        header_->rcu_lock.reader_unlock();
        ProcletManager::wait_until(header_, kPresent);
        goto retry;
      }
  });

  header_->rcu_lock.reader_unlock();
  if constexpr (std::is_same_v<RetT, void>) {
    f();
  } else {
    return f();
  }
}

inline void MigrationGuard::reset() {
  if (header_) {
    header_->rcu_lock.reader_unlock();
    header_ = nullptr;
  }
}

inline void MigrationGuard::release() { header_ = nullptr; }

}  // namespace nu

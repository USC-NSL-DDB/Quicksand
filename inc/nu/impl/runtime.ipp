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
#include "nu/stack_manager.hpp"

namespace nu {

inline SlabAllocator *Runtime::runtime_slab() { return runtime_slab_; }

inline StackManager *Runtime::stack_manager() { return stack_manager_; }

inline ArchivePool<> *Runtime::archive_pool() { return archive_pool_; }

inline RPCClientMgr *Runtime::rpc_client_mgr() { return rpc_client_mgr_; }

inline RPCServer *Runtime::rpc_server() { return rpc_server_; }

inline ProcletManager *Runtime::proclet_manager() { return proclet_manager_; }

inline PressureHandler *Runtime::pressure_handler() {
  return pressure_handler_;
}

inline ControllerClient *Runtime::controller_client() {
  return controller_client_;
}

inline ProcletServer *Runtime::proclet_server() { return proclet_server_; }

inline ResourceReporter *Runtime::resource_reporter() {
  return resource_reporter_;
}

inline Caladan *Runtime::caladan() { return caladan_; }

inline Migrator *Runtime::migrator() { return migrator_; }

inline ControllerServer *Runtime::controller_server() {
  return controller_server_;
}

inline SlabAllocator *Runtime::switch_slab(SlabAllocator *slab) {
  return caladan_->thread_set_proclet_slab(slab);
}

inline SlabAllocator *Runtime::switch_to_runtime_slab() {
  return caladan_->thread_set_proclet_slab(nullptr);
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
  auto *runtime_rsp = caladan_->thread_get_runtime_stack_base();
  switch_stack(runtime_rsp);
}

inline VAddrRange Runtime::get_proclet_stack_range(thread_t *thread) {
  VAddrRange range;
  auto rsp = caladan_->thread_get_rsp(thread);
  range.start = rsp - kStackRedZoneSize;
  range.end = ((rsp + kStackSize) & (~(kStackSize - 1)));
  return range;
}

inline SlabAllocator *Runtime::get_current_proclet_slab() {
  return caladan_->thread_get_proclet_slab();
}

inline ProcletHeader *Runtime::get_current_proclet_header() {
  return caladan_->thread_get_owner_proclet();
}

template <typename T>
inline T *Runtime::get_current_root_obj() {
  auto *proclet_header = get_current_proclet_header();
  if (!proclet_header) {
    return nullptr;
  }
  return reinterpret_cast<T *>(proclet_header->root_obj);
}

template <typename T>
inline T *Runtime::get_root_obj(ProcletID id) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(to_proclet_base(id));
  return reinterpret_cast<T *>(proclet_header->root_obj);
}

template <typename Cls, typename... A0s, typename... A1s>
__attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::__run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                  A1s &&...args) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  auto optional_migration_guard = attach_and_disable_migration(proclet_header);
  if (unlikely(!optional_migration_guard)) {
    return false;
  }
  auto &migration_guard = *optional_migration_guard;

  auto *obj_ptr = get_current_root_obj<Cls>();
  fn(&migration_guard, obj_ptr, std::forward<A1s>(args)...);
  detach();

  if (unlikely(caladan_->thread_has_been_migrated())) {
    migration_guard.reset();
    auto proclet_stack_base = get_proclet_stack_range(__self).end;
    switch_stack(caladan_->thread_get_runtime_stack_base());
    stack_manager_->put(reinterpret_cast<uint8_t *>(proclet_stack_base));
    get_runtime()->caladan()->thread_exit();
  }

  return true;
}

// By default, fn will be invoked with preemption disabled.
template <typename Cls, typename... A0s, typename... A1s>
__attribute__((optimize("no-omit-frame-pointer"))) bool
Runtime::run_within_proclet_env(void *proclet_base, void (*fn)(A0s...),
                                A1s &&...args) {
  bool ret;
  auto *proclet_stack = stack_manager_->get();
  assert(reinterpret_cast<uintptr_t>(proclet_stack) % kStackAlignment == 0);
  auto *old_rsp = switch_stack(proclet_stack);
  ret = __run_within_proclet_env<Cls>(proclet_base, fn,
                                      std::forward<A1s>(args)...);
  switch_stack(old_rsp);
  stack_manager_->put(proclet_stack);
  return ret;
}

template <typename T>
inline WeakProclet<T> Runtime::get_current_weak_proclet() {
  return WeakProclet<T>(to_proclet_id(get_current_proclet_header()));
}

inline void Runtime::detach() {
  Caladan::PreemptGuard g;

  assert(caladan_->thread_get_owner_proclet());
  get_current_proclet_header()->thread_cnt.dec(g);
  caladan_->thread_unset_owner_proclet(caladan_->thread_self(), true);
}

inline std::optional<MigrationGuard> Runtime::__attach_and_disable_migration(
    ProcletHeader *new_header, const Caladan::PreemptGuard &g) {
  caladan_->thread_set_owner_proclet(caladan_->thread_self(), new_header, true);
  barrier();

  if (new_header->is_local()) {
    new_header->rcu_lock.reader_lock(g);
    if (likely(new_header->is_local())) {
      new_header->thread_cnt.inc(g);
      return MigrationGuard(new_header);
    }
    new_header->rcu_lock.reader_unlock(g);
  }

  if (unlikely(new_header->status() == kMigrating &&
               caladan()->thread_is_rcu_held(Caladan::thread_self(),
                                             &new_header->rcu_lock))) {
    new_header->rcu_lock.reader_lock(g);
    new_header->thread_cnt.inc(g);
    return MigrationGuard(new_header);
  }

  return std::nullopt;
}

inline std::optional<MigrationGuard> Runtime::attach_and_disable_migration(
    ProcletHeader *proclet_header) {
  Caladan::PreemptGuard g;

  assert(!caladan_->thread_get_owner_proclet());
  auto guard = __attach_and_disable_migration(proclet_header, g);
  if (unlikely(!guard)) {
    caladan_->thread_unset_owner_proclet(caladan_->thread_self(), false);
  }
  return guard;
}

inline std::optional<MigrationGuard> Runtime::reattach_and_disable_migration(
    ProcletHeader *new_header, const MigrationGuard &old_guard) {
  Caladan::PreemptGuard g;

  auto *old_header = old_guard.header();
  old_header->thread_cnt.dec(g);
  assert(caladan_->thread_get_owner_proclet() == old_header);
  auto guard = __attach_and_disable_migration(new_header, g);
  if (unlikely(!guard)) {
    caladan_->thread_set_owner_proclet(caladan_->thread_self(), old_header,
                                       false);
    old_header->thread_cnt.inc(g);
  }
  return guard;
}

inline RuntimeSlabGuard::RuntimeSlabGuard() {
  original_slab_ = get_runtime()->switch_to_runtime_slab();
}

inline RuntimeSlabGuard::~RuntimeSlabGuard() {
  get_runtime()->switch_slab(original_slab_);
}

inline ProcletSlabGuard::ProcletSlabGuard(SlabAllocator *slab) {
  original_slab_ = get_runtime()->switch_slab(slab);
}

inline ProcletSlabGuard::~ProcletSlabGuard() {
  get_runtime()->switch_slab(original_slab_);
}

inline MigrationGuard::MigrationGuard() {
  Caladan::PreemptGuard g;

  header_ = get_runtime()->get_current_proclet_header();
  if (header_) {
  retry:
    auto nesting_cnt = header_->rcu_lock.reader_lock(g);
    if (unlikely(!header_->is_local() && nesting_cnt == 1)) {
      header_->rcu_lock.reader_unlock(g);
      g.enable_for([&] { ProcletManager::wait_until_being_local(header_); });
      goto retry;
    }
  }
}

inline MigrationGuard::MigrationGuard(ProcletHeader *header)
    : header_(header) {}

inline MigrationGuard::MigrationGuard(MigrationGuard &&o) : header_(o.header_) {
  o.header_ = nullptr;
}

inline MigrationGuard &MigrationGuard::operator=(MigrationGuard &&o) {
  reset();
  header_ = o.header_;
  o.header_ = nullptr;
  return *this;
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
      if (unlikely(!header_->is_local() && nesting_cnt == 1)) {
        header_->rcu_lock.reader_unlock();
        ProcletManager::wait_until_being_local(header_);
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

inline void runtime_check(Runtime *runtime) {
#ifdef DEBUG
  if (Caladan::thread_self()) {
    auto *proclet_header = runtime->caladan()->thread_get_owner_proclet();
    if (proclet_header && proclet_header->migratable &&
        runtime->caladan()->preempt_enabled()) {
      assert(runtime->caladan()->thread_is_rcu_held(Caladan::thread_self(),
                                                    &proclet_header->rcu_lock));
    }
  }
#endif
}

inline Runtime *get_runtime_nocheck() {
  static std::byte buf[sizeof(Runtime)];

  return reinterpret_cast<Runtime *>(buf);
}

inline Runtime *get_runtime() {
  auto *runtime = get_runtime_nocheck();
  runtime_check(runtime);
  return runtime;
}

}  // namespace nu

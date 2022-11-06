#include <memory>
#include <type_traits>
#include <utility>
#include <optional>

#include <net.h>
#include <sync.h>
#include <thread.h>

#include "nu/ctrl.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_mgr.hpp"
#include "nu/runtime.hpp"
#include "nu/type_traits.hpp"

namespace nu {

template <typename Cls, typename... As>
void ProcletServer::__construct_proclet(MigrationGuard *callee_guard, Cls *obj,
                                        cereal::BinaryInputArchive &ia,
                                        RPCReturner returner) {
  auto *callee_header = callee_guard->header();
  auto &callee_slab = callee_header->slab;
  auto obj_space = callee_slab.yield(sizeof(Cls));

  std::tuple<std::decay_t<As>...> args{std::decay_t<As>()...};

  callee_guard->enable_for([&] {
    // FIXME: a quick hack here that avoids nested RCUs.
    std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);
    ProcletSlabGuard slab_guard(&callee_slab);
    std::apply(
        [&](auto &&... args) { new (obj_space) Cls(std::move(args)...); },
        args);
  });

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp_ok(oa_sstream, &returner);
}

template <typename Cls, typename... As>
void ProcletServer::construct_proclet(cereal::BinaryInputArchive &ia,
                                      RPCReturner *returner) {
  void *base;
  uint64_t size;
  bool pinned;
  ia >> base >> size >> pinned;

  Runtime::proclet_manager->setup(base, size, /* migratable = */ !pinned,
                                  /* from_migration = */ false);

  auto *proclet_header = reinterpret_cast<ProcletHeader *>(base);
  proclet_header->status() = kPresent;

  bool proclet_not_found = !Runtime::run_within_proclet_env<Cls>(
      base, __construct_proclet<Cls, As...>, ia, *returner);
  BUG_ON(proclet_not_found);

  Runtime::proclet_manager->insert(base);
}

template <typename Cls, typename... As>
void ProcletServer::construct_proclet_locally(
    MigrationGuard *caller_guard, const RuntimeSlabGuard &runtime_slab_guard,
    void *base, uint64_t size, bool pinned, As &&... args) {
  Runtime::proclet_manager->setup(base, size, /* migratable = */ !pinned,
                                  /* from_migration = */ false);

  auto *callee_header = reinterpret_cast<ProcletHeader *>(base);
  callee_header->status() = kPresent;
  auto &callee_slab = callee_header->slab;
  auto obj_space = callee_slab.yield(sizeof(Cls));

  auto *caller_header = Runtime::get_current_proclet_header();
  auto optional_callee_guard =
      Runtime::reattach_and_disable_migration(callee_header, *caller_guard);
  BUG_ON(!optional_callee_guard);
  auto &callee_guard = *optional_callee_guard;

  {
    ProcletSlabGuard slab_guard(&callee_header->slab);

    // Do copy for the most cases and only do move when we are sure it's safe.
    // For copy, we assume the type implements "deep copy".
    using ArgsTuple = std::tuple<std::decay_t<As>...>;
    ArgsTuple copied_args(move_if_safe(std::forward<As>(args))...);

    barrier();
    {
      RuntimeSlabGuard slab_guard;
      Runtime::proclet_manager->insert(base);
    }
    caller_guard->reset();

    callee_guard.enable_for([&] {
      std::apply(
          [&](auto &&... args) { new (obj_space) Cls(std::move(args)...); },
          copied_args);
    });
  }

  auto optional =
      Runtime::reattach_and_disable_migration(caller_header, callee_guard);
  if (!optional) {
    RPCReturnBuffer return_buf;
    Migrator::migrate_thread_and_ret_val<void>(
        &callee_guard, std::move(return_buf), to_proclet_id(caller_header),
        nullptr, nullptr);
  }
}

template <typename Cls>
void ProcletServer::__update_ref_cnt(MigrationGuard *callee_guard, Cls *obj,
                                     RPCReturner returner, int delta,
                                     bool *destructed) {
  auto *proclet_header = callee_guard->header();
  proclet_header->spin_lock.lock();
  auto latest_cnt = (proclet_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  proclet_header->spin_lock.unlock();
  *destructed = (latest_cnt == 0);

  if (*destructed) {
    while (unlikely(
        !Runtime::proclet_manager->remove_for_destruction(proclet_header))) {
      // Will be migrated at this point, so let's wait for migration to finish.
      callee_guard->enable_for([] {});
    }

    // Now won't be migrated.
    ProcletSlabGuard slab_guard(&proclet_header->slab);
    callee_guard->enable_for([&] { obj->~Cls(); });
    proclet_header->status() = kAbsent;
  }

  RuntimeSlabGuard guard;
  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp_ok(oa_sstream, &returner);
}

template <typename Cls>
void ProcletServer::update_ref_cnt(cereal::BinaryInputArchive &ia,
                                   RPCReturner *returner) {
  ProcletID id;
  ia >> id;
  int delta;
  ia >> delta;

  auto *proclet_base = to_proclet_base(id);
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);

  bool destructed = false;
  bool proclet_not_found = !Runtime::run_within_proclet_env<Cls>(
      proclet_base, __update_ref_cnt<Cls>, *returner, delta, &destructed);

  if (destructed) {
    // Wait for other concurrent cnt updating threads to finish.
    proclet_header->rcu_lock.writer_sync();
    auto vaddr_range = proclet_header->range();
    ProcletServer::release_proclet(vaddr_range);
    Runtime::proclet_manager->cleanup(proclet_base,
                                      /* for_migration = */ false);
  }

  if (proclet_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls>
void ProcletServer::update_ref_cnt_locally(MigrationGuard *callee_guard,
                                           ProcletHeader *caller_header,
                                           ProcletHeader *callee_header,
                                           int delta) {
  callee_header->spin_lock.lock();
  auto latest_cnt = (callee_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  callee_header->spin_lock.unlock();

  RuntimeSlabGuard runtime_slab_guard;

  if (latest_cnt == 0) {
    while (unlikely(
        !Runtime::proclet_manager->remove_for_destruction(callee_header))) {
      // Will be migrated at this point, so let's wait for migration to finish.
      callee_guard->enable_for([] {});
    }

    // Now won't be migrated.
    auto *obj = Runtime::get_root_obj<Cls>(to_proclet_id(callee_header));
    ProcletSlabGuard callee_slab_guard(&callee_header->slab);
    callee_guard->enable_for([&] { obj->~Cls(); });
    callee_header->status() = kAbsent;
  }

  auto optional =
      Runtime::reattach_and_disable_migration(caller_header, *callee_guard);

  if (latest_cnt == 0) {
    // Wait for other concurrent cnt updating threads to finish.
    callee_guard->reset();
    callee_header->rcu_lock.writer_sync();
    auto vaddr_range = callee_header->range();
    ProcletServer::release_proclet(vaddr_range);
    Runtime::proclet_manager->cleanup(callee_header,
                                      /* for_migration = */ false);
  }

  if (!optional) {
    RPCReturnBuffer return_buf;
    Migrator::migrate_thread_and_ret_val<void>(
        callee_guard, std::move(return_buf), to_proclet_id(caller_header),
        nullptr, nullptr);
  }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ProcletServer::__run_closure(MigrationGuard *callee_guard, Cls *obj,
                                  cereal::BinaryInputArchive &ia,
                                  RPCReturner returner) {
  constexpr auto kNonVoidRetT = !std::is_same<RetT, void>::value;
  std::conditional_t<kNonVoidRetT, RetT, ErasedType> ret;

  auto *callee_header = callee_guard->header();
  callee_header->cpu_load.start_monitor();
  callee_header->thread_cnt.inc_unsafe();

  {
    ProcletSlabGuard slab_guard(&callee_header->slab);

    FnPtr fn;
    ia >> fn;

    std::tuple<std::decay_t<S1s>...> states{std::decay_t<S1s>()...};

    callee_guard->enable_for([&] {
      // FIXME: a quick hack here that avoids nested RCUs.
      std::apply([&](auto &&... states) { ((ia >> states), ...); }, states);
      std::apply(
          [&](auto &&... states) {
            if constexpr (kNonVoidRetT) {
              ret = fn(*obj, std::move(states)...);
            } else {
              fn(*obj, std::move(states)...);
            }
          },
          states);
    });
  }

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  if constexpr (kNonVoidRetT) {
    oa_sstream->oa << std::move(ret);
  }
  send_rpc_resp_ok(oa_sstream, &returner);

  callee_header->thread_cnt.dec_unsafe();
  callee_header->cpu_load.end_monitor();
}

#pragma GCC diagnostic pop

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ProcletServer::run_closure(cereal::BinaryInputArchive &ia,
                                RPCReturner *returner) {
  ProcletID id;
  ia >> id;
  auto *proclet_header = to_proclet_header(id);

  bool proclet_not_found = !Runtime::run_within_proclet_env<Cls>(
      proclet_header, __run_closure<Cls, RetT, FnPtr, S1s...>, ia, *returner);

  if (proclet_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ProcletServer::run_closure_locally(
    MigrationGuard *callee_migration_guard,
    const ProcletSlabGuard &callee_slab_guard, RetT *caller_ptr,
    ProcletHeader *caller_header, ProcletHeader *callee_header, FnPtr fn_ptr,
    S1s &&... states) {
  callee_header->cpu_load.start_monitor();
  callee_header->thread_cnt.inc_unsafe();

  auto *obj = Runtime::get_root_obj<Cls>(to_proclet_id(callee_header));

  if constexpr (!std::is_same<RetT, void>::value) {
    auto ret = callee_migration_guard->enable_for(
        [&] { return fn_ptr(*obj, std::move(states)...); });
    callee_header->thread_cnt.dec_unsafe();
    callee_header->cpu_load.end_monitor();

    auto optional = Runtime::reattach_and_disable_migration(
        caller_header, *callee_migration_guard);
    if (likely(optional)) {
      *caller_ptr = move_if_safe(std::move(ret));
      return;
    }

    RuntimeSlabGuard slab_guard;

    auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
    oa_sstream->oa << std::move(ret);
    auto ss_view = oa_sstream->ss.view();
    auto ret_val_span = std::span<const std::byte>(
        reinterpret_cast<const std::byte *>(ss_view.data()),
        oa_sstream->ss.tellp());
    RPCReturnBuffer ret_val_buf(ret_val_span);

    Migrator::migrate_thread_and_ret_val<RetT>(
        callee_migration_guard, std::move(ret_val_buf),
        to_proclet_id(caller_header), caller_ptr,
        [&] { Runtime::archive_pool->put_oa_sstream(oa_sstream); });
  } else {
    callee_migration_guard->enable_for(
        [&] { fn_ptr(*obj, std::move(states)...); });
    callee_header->thread_cnt.dec_unsafe();
    callee_header->cpu_load.end_monitor();

    auto optional = Runtime::reattach_and_disable_migration(
        caller_header, *callee_migration_guard);
    if (likely(optional)) {
      return;
    }

    RuntimeSlabGuard slab_guard;
    RPCReturnBuffer ret_val_buf;

    callee_migration_guard->reset();

    Migrator::migrate_thread_and_ret_val<void>(
        callee_migration_guard, std::move(ret_val_buf),
        to_proclet_id(caller_header), nullptr, nullptr);
  }
}

}  // namespace nu

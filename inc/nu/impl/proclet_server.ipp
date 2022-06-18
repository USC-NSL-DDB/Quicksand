#include <alloca.h>
#include <net.h>
#include <sync.h>
#include <thread.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "nu/ctrl.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_mgr.hpp"
#include "nu/runtime.hpp"
#include "nu/type_traits.hpp"

namespace nu {

template <typename Cls, typename... As>
void ProcletServer::construct_proclet(cereal::BinaryInputArchive &ia,
                                      RPCReturner *returner) {
  void *base;
  bool pinned;
  ia >> base >> pinned;

  Runtime::proclet_manager->allocate(base, /* migratable = */ !pinned);

  auto *proclet_header = reinterpret_cast<ProcletHeader *>(base);
  proclet_header->cpu_load.reset();
  auto &slab = proclet_header->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  std::tuple<std::decay_t<As>...> args;
  std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);
  std::apply(
      [&](auto &&... args) {
        ProcletSlabGuard proclet_slab_guard(&slab);
        proclet_header->status = kPresent;
        auto *self = thread_self();
        auto *old_owner = thread_set_owner_proclet(self, base);
        new (obj_space) Cls(std::move(args)...);
        thread_set_owner_proclet(self, old_owner);
      },
      args);

  Runtime::proclet_manager->insert(base);

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp_ok(oa_sstream, returner);
}

template <typename Cls, typename... As>
void ProcletServer::construct_proclet_locally(
    MigrationDisabledGuard *caller_guard, void *base, bool pinned,
    As &&... args) {
  RuntimeSlabGuard runtime_slab_guard;
  Runtime::proclet_manager->allocate(base, /* migratable = */ !pinned);

  auto *callee_header = reinterpret_cast<ProcletHeader *>(base);
  callee_header->cpu_load.reset();
  callee_header->status = kPresent;
  auto &slab = callee_header->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  auto *caller_header = caller_guard->get_proclet_header();
  {
    ProcletSlabGuard proclet_slab_guard(&slab);

    using ArgsTuple = std::tuple<std::decay_t<As>...>;
    // Do copy for the most cases and only do move when we are sure it's safe.
    // For copy, we assume the type implements "deep copy".
    ArgsTuple copied_args(move_if_safe(std::forward<As>(args))...);
    thread_set_owner_proclet(thread_self(), base);
    // Save to drop the caller migration guard now.
    caller_guard->reset();

    std::apply(
        [&](auto &&... args) { new (obj_space) Cls(std::move(args)...); },
        copied_args);
  }

  NonBlockingMigrationDisabledGuard guard(caller_header);
  if (caller_header && unlikely(!guard)) {
    RPCReturnBuffer return_buf;
    Migrator::migrate_thread_and_ret_val<void>(
        std::move(return_buf), to_proclet_id(caller_header), nullptr, nullptr);
  } else {
    thread_set_owner_proclet(thread_self(), caller_header);
  }

  Runtime::proclet_manager->insert(base);
}

template <typename Cls>
void ProcletServer::__update_ref_cnt(Cls &obj, RPCReturner returner,
                                     ProcletHeader *proclet_header, int delta,
                                     bool *deallocate) {
  proclet_header->spin.Lock();
  auto latest_cnt = (proclet_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  proclet_header->spin.Unlock();

  if (latest_cnt == 0) {
    if (likely(
            Runtime::proclet_manager->remove_for_destruction(proclet_header))) {
      // Won't be migrated at this point.
      *deallocate = true;
      {
        MigrationEnabledGuard guard;
        obj.~Cls();
      }
    } else {
      // Will be migrated at this point, so we wait for migration to be
      // finished.
      {
        MigrationEnabledGuard guard;
        ProcletManager::wait_until_present(proclet_header);
      }
      // Safe without acquiring the lock since the proclet is dead now.
      proclet_header->ref_cnt = -delta;
      RuntimeSlabGuard guard;
      send_rpc_resp_wrong_client(&returner);
      return;
    }
  }

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

  bool deallocate = false;
  bool proclet_not_found = !Runtime::run_within_proclet_env<Cls>(
      proclet_base, __update_ref_cnt<Cls>, *returner, proclet_header, delta,
      &deallocate);

  if (deallocate) {
    // Wait for all ongoing invocations to finish.
    proclet_header->rcu_lock.writer_sync();
    Runtime::proclet_manager->deallocate(proclet_base);
  }

  if (proclet_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls>
bool ProcletServer::update_ref_cnt_locally(
    NonBlockingMigrationDisabledGuard *callee_guard, ProcletID id, int delta) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(to_proclet_base(id));
  proclet_header->spin.Lock();
  auto latest_cnt = (proclet_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  proclet_header->spin.Unlock();

  if (latest_cnt == 0) {
    if (unlikely(!Runtime::proclet_manager->remove_for_destruction(
            proclet_header))) {
      return false;
    }
    // Won't be migrated at this point.
    {
      auto *obj = Runtime::get_root_obj<Cls>(id);
      ProcletSlabGuard proclet_slab_guard(&proclet_header->slab);
      callee_guard->reset();
      obj->~Cls();
    }

    RuntimeSlabGuard runtime_slab_guard;
    Runtime::proclet_manager->deallocate(proclet_header);
  }

  return true;
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ProcletServer::__run_closure(Cls &obj, ProcletHeader *proclet_header,
                                  cereal::BinaryInputArchive &ia,
                                  RPCReturner returner) {
  auto state = proclet_header->cpu_load.monitor_start();
  proclet_header->thread_cnt.inc_unsafe();

  decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;

  FnPtr fn;
  ia >> fn;

  // TODO: refactor this. States part looks ugly since for now we want to avoid
  // nested RCU locks.
  using States = std::tuple<std::decay_t<S1s>...>;
  auto *states = reinterpret_cast<States *>(alloca(sizeof(States)));
  std::construct_at(states);
  std::apply([&](auto &&... states) { ((ia >> states), ...); }, *states);

  if constexpr (std::is_same<RetT, void>::value) {
    {
      MigrationEnabledGuard guard;
      std::apply([&](auto &&... states) { fn(obj, std::move(states)...); },
                 *states);
      std::destroy_at(states);
    }
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
  } else {
    MigrationEnabledGuard guard;
    auto ret = std::apply(
        [&](auto &&... states) { return fn(obj, std::move(states)...); },
        *states);
    std::destroy_at(states);
    guard.reset();

    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    oa_sstream->oa << ret;
  }

  send_rpc_resp_ok(oa_sstream, &returner);
  proclet_header->thread_cnt.dec_unsafe();
  proclet_header->cpu_load.monitor_end(state);
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ProcletServer::run_closure(cereal::BinaryInputArchive &ia,
                                RPCReturner *returner) {
  ProcletID id;
  ia >> id;
  auto *proclet_header = to_proclet_header(id);
  bool proclet_not_found = !Runtime::run_within_proclet_env<Cls>(
      proclet_header, __run_closure<Cls, RetT, FnPtr, S1s...>, proclet_header,
      ia, *returner);
  if (proclet_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ProcletServer::run_closure_locally(RetT *caller_ptr, ProcletID caller_id,
                                        ProcletID callee_id, FnPtr fn_ptr,
                                        S1s &&... states) {
  auto *callee_proclet_header = to_proclet_header(callee_id);
  auto *caller_proclet_header = to_proclet_header(caller_id);
  auto state = callee_proclet_header->cpu_load.monitor_start();
  callee_proclet_header->thread_cnt.inc_unsafe();

  auto *obj = Runtime::get_root_obj<Cls>(callee_id);
  if constexpr (!std::is_same<RetT, void>::value) {
    auto *ret = reinterpret_cast<RetT *>(alloca(sizeof(RetT)));
    std::construct_at(ret);
    *ret = fn_ptr(*obj, std::move(states)...);
    callee_proclet_header->thread_cnt.dec_unsafe();
    callee_proclet_header->cpu_load.monitor_end(state);

    {
      NonBlockingMigrationDisabledGuard caller_guard(caller_proclet_header);

      if (likely(caller_guard)) {
        {
          ProcletSlabGuard caller_slab_guard(&caller_proclet_header->slab);
          *caller_ptr = move_if_safe(std::move(*ret));
        }
        thread_set_owner_proclet(thread_self(), caller_proclet_header);
        std::destroy_at(ret);
        return;
      }
    }

    decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    oa_sstream->oa << *ret;
    auto ss_view = oa_sstream->ss.view();
    auto ret_val_span = std::span<const std::byte>(
        reinterpret_cast<const std::byte *>(ss_view.data()),
        oa_sstream->ss.tellp());
    RPCReturnBuffer ret_val_buf(ret_val_span);
    std::destroy_at(ret);

    RuntimeSlabGuard slab_guard;
    Migrator::migrate_thread_and_ret_val<RetT>(
        std::move(ret_val_buf), caller_id, caller_ptr, [&, th = thread_self()] {
          // FIXME
          // if (thread_is_migrated(th)) {
          // callee_proclet_header->migrated_wg.Done();
          // }
          Runtime::archive_pool->put_oa_sstream(oa_sstream);
        });
  } else {
    fn_ptr(*obj, std::move(states)...);
    callee_proclet_header->thread_cnt.dec_unsafe();
    callee_proclet_header->cpu_load.monitor_end(state);

    {
      NonBlockingMigrationDisabledGuard caller_guard(caller_proclet_header);

      if (likely(caller_guard)) {
        thread_set_owner_proclet(thread_self(), caller_proclet_header);
        return;
      }
    }

    RuntimeSlabGuard slab_guard;
    RPCReturnBuffer ret_val_buf;
    Migrator::migrate_thread_and_ret_val<void>(
        std::move(ret_val_buf), caller_id, nullptr, [&, th = thread_self()] {
          // FIXME
          // if (thread_is_migrated(th))
          // {
          //   callee_proclet_header->migrated_wg.Done();
          // }
        });
  }
}

}  // namespace nu

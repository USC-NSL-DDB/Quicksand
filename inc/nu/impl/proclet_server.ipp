#include <alloca.h>
#include <net.h>

#include <memory>
#include <optional>
#include <syncstream>
#include <type_traits>
#include <utility>

#include "nu/ctrl.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_mgr.hpp"
#include "nu/runtime.hpp"
#include "nu/type_traits.hpp"

#ifdef DDB_SUPPORT
#include <ddb/backtrace.hpp>
#endif

namespace nu {

constexpr static bool kDumpProcletInfoOnDestruction = false;

template <typename Cls, typename... As>
void ProcletServer::__construct_proclet(MigrationGuard *callee_guard, Cls *obj,
                                        ArchivePool<>::IASStream *ia_sstream,
                                        RPCReturner returner) {
  auto *callee_header = callee_guard->header();
  auto &callee_slab = callee_header->slab;
  callee_header->root_obj = callee_slab.yield(sizeof(Cls));

  {
    ProcletSlabGuard slab_guard(&callee_slab);

#ifdef DDB_SUPPORT
    DDB::DDBTraceMeta meta;
    ia_sstream->ia >> meta;
    // std::osyncstream synced_out(std::cout);
    // synced_out << "__construct_proclet: " << meta << std::endl;
    // synced_out.emit(); // Flush the buffer
    BUG_ON(!meta.valid());
#endif

    using ArgsTuple = std::tuple<std::decay_t<As>...>;
    auto *args = reinterpret_cast<ArgsTuple *>(alloca(sizeof(ArgsTuple)));
    new (args) ArgsTuple();
    std::apply([&](auto &&...args) { ((ia_sstream->ia >> args), ...); }, *args);
    
    auto construct_fn = [&]() {
      std::apply(
          [&](auto &&...args) {
            new (callee_header->root_obj) Cls(std::move(args)...);
          },
          *args);
    };

    callee_guard->enable_for([&] {
      // std::apply(
      //     [&](auto &&...args) {
      //       new (callee_header->root_obj) Cls(std::move(args)...);
      //     },
      //     *args);
#ifdef DDB_SUPPORT
      DDB::Backtrace::extraction(
          [&]() -> DDB::DDBTraceMeta {
            return meta;
          },
          construct_fn);
#else
      construct_fn();  // Call the defined lambda here
#endif
      
      std::destroy_at(args);
    });
  }

  auto *oa_sstream = get_runtime()->archive_pool()->get_oa_sstream();
  get_runtime()->send_rpc_resp_ok(oa_sstream, ia_sstream, &returner);
}

template <typename Cls, typename... As>
void ProcletServer::construct_proclet(ArchivePool<>::IASStream *ia_sstream,
                                      RPCReturner *returner) {
  void *base;
  uint64_t size;
  bool pinned;
  ia_sstream->ia >> base >> size >> pinned;

  get_runtime()->proclet_manager()->setup(base, size,
                                          /* migratable = */ !pinned,
                                          /* from_migration = */ false);

  auto *proclet_header = reinterpret_cast<ProcletHeader *>(base);
  proclet_header->status() = kPresent;

  bool proclet_not_found = !get_runtime()->run_within_proclet_env<Cls>(
      base, __construct_proclet<Cls, As...>, ia_sstream, *returner);
  BUG_ON(proclet_not_found);

  get_runtime()->proclet_manager()->insert(base);
}

template <typename Cls, typename... As>
void ProcletServer::construct_proclet_locally(MigrationGuard &&caller_guard,
                                              void *base, uint64_t size,
                                              bool pinned, As &&...args) {
  std::optional<MigrationGuard> optional_caller_guard;
  RuntimeSlabGuard slab_guard;
  get_runtime()->proclet_manager()->setup(base, size,
                                          /* migratable = */ !pinned,
                                          /* from_migration = */ false);

  auto *callee_header = reinterpret_cast<ProcletHeader *>(base);
  callee_header->status() = kPresent;
  auto &callee_slab = callee_header->slab;
  callee_header->root_obj = callee_slab.yield(sizeof(Cls));

  auto *caller_header = get_runtime()->get_current_proclet_header();
  auto optional_callee_guard = get_runtime()->reattach_and_disable_migration(
      callee_header, caller_guard);
  BUG_ON(!optional_callee_guard);
  auto &callee_guard = *optional_callee_guard;

  {
    ProcletSlabGuard slab_guard(&callee_header->slab);

    // Do copy for the most cases and only do move when we are sure it's safe.
    // For copy, we assume the type implements "deep copy".
    using ArgsTuple = std::tuple<std::decay_t<As>...>;
    auto *copied_args =
        reinterpret_cast<ArgsTuple *>(alloca(sizeof(ArgsTuple)));
    new (copied_args) ArgsTuple(pass_across_proclet(std::forward<As>(args))...);

    barrier();
    {
      RuntimeSlabGuard slab_guard;
      get_runtime()->proclet_manager()->insert(base);
    }
    caller_guard.reset();

    callee_guard.enable_for([&] {
      std::apply(
          [&](auto &&...args) {
            new (callee_header->root_obj) Cls(std::move(args)...);
          },
          *copied_args);
      std::destroy_at(copied_args);
    });
  }

  optional_caller_guard = get_runtime()->reattach_and_disable_migration(
      caller_header, callee_guard);
  if (!optional_caller_guard) {
    get_runtime()->detach();
    callee_guard.reset();

    RPCReturnBuffer return_buf;
    caller_guard = Migrator::migrate_thread_and_ret_val<void>(
        std::move(return_buf), to_proclet_id(caller_header), nullptr, nullptr);
  }
}

template <typename Cls>
void ProcletServer::__update_ref_cnt(MigrationGuard *callee_guard, Cls *obj,
                                     ArchivePool<>::IASStream *ia_sstream,
                                     RPCReturner returner, int delta,
                                     bool *destructed) {
  auto *proclet_header = callee_guard->header();
  auto update_ref_cnt_fn = [&]() {
    proclet_header->spin_lock.lock();
    auto latest_cnt = (proclet_header->ref_cnt += delta);
    BUG_ON(latest_cnt < 0);
    proclet_header->spin_lock.unlock();
    *destructed = (latest_cnt == 0);
  };

  DDB::DDBTraceMeta meta;
  ia_sstream->ia >> meta;
  BUG_ON(!meta.valid());

#ifdef DDB_SUPPORT
  DDB::Backtrace::extraction(
      [&]() -> DDB::DDBTraceMeta {
        return meta;
      },
      update_ref_cnt_fn);
#else
  update_ref_cnt_fn();  // Call the defined lambda here
#endif

  // auto latest_cnt = (proclet_header->ref_cnt += delta);
  // BUG_ON(latest_cnt < 0);
  // proclet_header->spin_lock.unlock();
  // *destructed = (latest_cnt == 0);

  if (*destructed) {
    while (unlikely(!get_runtime()->proclet_manager()->remove_for_destruction(
        proclet_header))) {
      // Will be migrated at this point, so let's wait for migration to finish.
      callee_guard->enable_for([] {});
    }

    if constexpr (kDumpProcletInfoOnDestruction) {
      Caladan::PreemptGuard g;

      std::osyncstream synced_out(std::cout);
      synced_out << "Info: " << typeid(Cls).name() << " " << proclet_header
                 << " " << proclet_header->slab.get_cur_usage() << " "
                 << proclet_header->cpu_load.get_avg_load() << std::endl;
    }

    // Now won't be migrated.
    ProcletSlabGuard slab_guard(&proclet_header->slab);
    callee_guard->enable_for([&] { obj->~Cls(); });
  }

  auto *oa_sstream = get_runtime()->archive_pool()->get_oa_sstream();
  get_runtime()->send_rpc_resp_ok(oa_sstream, ia_sstream, &returner);
}

template <typename Cls>
void ProcletServer::update_ref_cnt(ArchivePool<>::IASStream *ia_sstream,
                                   RPCReturner *returner) {
  ProcletID id;
  int delta;
  ia_sstream->ia >> id >> delta;

  auto *proclet_base = to_proclet_base(id);
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);

  bool destructed = false;
  bool proclet_not_found = !get_runtime()->run_within_proclet_env<Cls>(
      proclet_base, __update_ref_cnt<Cls>, ia_sstream, *returner, delta,
      &destructed);

  if (destructed) {
    // Wait for other concurrent cnt updating threads to finish.
    proclet_header->rcu_lock.writer_sync();
    get_runtime()->proclet_manager()->cleanup(proclet_base,
                                              /* for_migration = */ false);
    get_runtime()->controller_client()->destroy_proclet(
        proclet_header->range());
  }

  if (proclet_not_found) {
    get_runtime()->send_rpc_resp_wrong_client(returner);
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

  std::optional<MigrationGuard> optional_caller_guard;
  RuntimeSlabGuard runtime_slab_guard;

  if (latest_cnt == 0) {
    while (unlikely(!get_runtime()->proclet_manager()->remove_for_destruction(
        callee_header))) {
      // Will be migrated at this point, so let's wait for migration to finish.
      callee_guard->enable_for([] {});
    }

    // Now won't be migrated.
    if constexpr (kDumpProcletInfoOnDestruction) {
      Caladan::PreemptGuard g;

      std::osyncstream synced_out(std::cout);
      synced_out << "Info: " << typeid(Cls).name() << " " << callee_header
                 << " " << callee_header->slab.get_cur_usage() << " "
                 << callee_header->cpu_load.get_avg_load() << std::endl;
    }

    auto *obj = get_runtime()->get_root_obj<Cls>(to_proclet_id(callee_header));
    {
      ProcletSlabGuard callee_slab_guard(&callee_header->slab);
      callee_guard->enable_for([&] {
        obj->~Cls();
        callee_header->rcu_lock.writer_sync();
      });
    }
    get_runtime()->proclet_manager()->cleanup(callee_header,
                                              /* for_migration = */ false);
    get_runtime()->controller_client()->destroy_proclet(callee_header->range());
  }

  optional_caller_guard = get_runtime()->reattach_and_disable_migration(
      caller_header, *callee_guard);
  if (!optional_caller_guard) {
    get_runtime()->detach();
    callee_guard->reset();

    RPCReturnBuffer return_buf;
    *optional_caller_guard = Migrator::migrate_thread_and_ret_val<void>(
        std::move(return_buf), to_proclet_id(caller_header), nullptr, nullptr);
  }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"

template <bool MigrEn, bool CPUMon, bool CPUSamp, typename Cls, typename RetT,
          typename FnPtr, typename... S1s>
void ProcletServer::__run_closure(MigrationGuard *callee_guard, Cls *obj,
                                  ArchivePool<>::IASStream *ia_sstream,
                                  RPCReturner returner) {
  auto *callee_header = callee_guard->header();
  ProcletSlabGuard callee_slab_guard(&callee_header->slab);

  if constexpr (CPUMon) {
    if constexpr (CPUSamp) {
      callee_header->cpu_load.start_monitor();
    } else {
      callee_header->cpu_load.start_monitor_no_sampling();
    }
  }

  constexpr auto kNonVoidRetT = !std::is_same<RetT, void>::value;
  std::conditional_t<kNonVoidRetT, RetT, ErasedType> ret;

  FnPtr fn;
  ia_sstream->ia >> fn;

#ifdef DDB_SUPPORT
  DDB::DDBTraceMeta meta;
  ia_sstream->ia >> meta;
  BUG_ON(!meta.valid());
#endif

  std::tuple<std::decay_t<S1s>...> states;
  std::apply([&](auto &&...states) { ((ia_sstream->ia >> states), ...); },
             states);
  
  auto __apply_fn = [&] {
    std::apply(
        [&](auto &&...states) {
          if constexpr (kNonVoidRetT) {
            ret = fn(*obj, std::move(states)...);
          } else {
            fn(*obj, std::move(states)...);
          }
        },
        states);
  };
             
  auto apply_fn = [&] {
#ifdef DDB_SUPPORT
    DDB::Backtrace::extraction(
        [&]() -> DDB::DDBTraceMeta {
          return meta;
        },
        __apply_fn);
#else
    __apply_fn();
#endif
    // std::apply(
    //     [&](auto &&...states) {
    //       if constexpr (kNonVoidRetT) {
    //         ret = fn(*obj, std::move(states)...);
    //       } else {
    //         fn(*obj, std::move(states)...);
    //       }
    //     },
    //     states);
  };

  if constexpr (MigrEn) {
    callee_guard->enable_for([&] { apply_fn(); });
  } else {
    apply_fn();
  }

  RuntimeSlabGuard runtime_slab_guard;

  auto *oa_sstream = get_runtime()->archive_pool()->get_oa_sstream();
  if constexpr (kNonVoidRetT) {
    oa_sstream->oa << std::move(ret);
  }
  get_runtime()->send_rpc_resp_ok(oa_sstream, ia_sstream, &returner);

  if constexpr (CPUMon) {
    callee_header->cpu_load.end_monitor();
  }
}

#pragma GCC diagnostic pop

template <bool MigrEn, bool CPUMon, bool CPUSamp, typename Cls, typename RetT,
          typename FnPtr, typename... S1s>
void ProcletServer::run_closure(ArchivePool<>::IASStream *ia_sstream,
                                RPCReturner *returner) {
  ProcletID id;
  ia_sstream->ia >> id;

  auto *proclet_header = to_proclet_header(id);

  bool proclet_not_found = !get_runtime()->run_within_proclet_env<Cls>(
      proclet_header,
      __run_closure<MigrEn, CPUMon, CPUSamp, Cls, RetT, FnPtr, S1s...>,
      ia_sstream, *returner);

  if (proclet_not_found) {
    get_runtime()->send_rpc_resp_wrong_client(returner);
  }
}

template <bool MigrEn, bool CPUMon, bool CPUSamp, typename Cls, typename RetT,
          typename FnPtr, typename... Ss>
void ProcletServer::run_closure_locally(
    MigrationGuard *caller_migration_guard,
    MigrationGuard *callee_migration_guard,
    const ProcletSlabGuard &callee_slab_guard, RetT *caller_ptr,
    ProcletHeader *caller_header, ProcletHeader *callee_header, FnPtr fn_ptr,
    std::tuple<Ss...> *states) {
  caller_migration_guard->reset();

  if constexpr (CPUMon) {
    if constexpr (CPUSamp) {
      callee_header->cpu_load.start_monitor();
    } else {
      callee_header->cpu_load.start_monitor_no_sampling();
    }
  }

  auto *obj = get_runtime()->get_root_obj<Cls>(to_proclet_id(callee_header));

  if constexpr (!std::is_same<RetT, void>::value) {
    auto *ret = reinterpret_cast<RetT *>(alloca(sizeof(RetT)));
    std::apply(
        [&](auto &&...states) {
          if constexpr (MigrEn) {
            callee_migration_guard->enable_for(
                [&] { new (ret) RetT(fn_ptr(*obj, std::move(states)...)); });
          } else {
            new (ret) RetT(fn_ptr(*obj, std::move(states)...));
          }
        },
        std::move(*states));
    std::destroy_at(states);
    if constexpr (CPUMon) {
      callee_header->cpu_load.end_monitor();
    }

    auto optional_caller_guard = get_runtime()->reattach_and_disable_migration(
        caller_header, *callee_migration_guard);
    if (likely(optional_caller_guard)) {
      ProcletSlabGuard slab_guard(&caller_header->slab);
      *caller_ptr = pass_across_proclet(std::move(*ret));
      std::destroy_at(ret);
      callee_migration_guard->reset();
      *caller_migration_guard = std::move(*optional_caller_guard);
      return;
    }
    get_runtime()->detach();

    RuntimeSlabGuard slab_guard;
    auto *oa_sstream = get_runtime()->archive_pool()->get_oa_sstream();
    oa_sstream->oa << std::move(*ret);
    auto ss_view = oa_sstream->ss.view();
    auto ret_val_span = std::span<const std::byte>(
        reinterpret_cast<const std::byte *>(ss_view.data()),
        oa_sstream->ss.tellp());
    RPCReturnBuffer ret_val_buf(ret_val_span);

    std::destroy_at(ret);
    callee_migration_guard->reset();

    *caller_migration_guard = Migrator::migrate_thread_and_ret_val<RetT>(
        std::move(ret_val_buf), to_proclet_id(caller_header), caller_ptr,
        [&] { get_runtime()->archive_pool()->put_oa_sstream(oa_sstream); });
    return;
  } else {
    std::apply(
        [&](auto &&...states) {
          if constexpr (MigrEn) {
            callee_migration_guard->enable_for(
                [&] { fn_ptr(*obj, std::move(states)...); });
          } else {
            fn_ptr(*obj, std::move(states)...);
          }
        },
        std::move(*states));
    std::destroy_at(states);
    if constexpr (CPUMon) {
      callee_header->cpu_load.end_monitor();
    }

    auto optional_caller_guard = get_runtime()->reattach_and_disable_migration(
        caller_header, *callee_migration_guard);
    if (likely(optional_caller_guard)) {
      callee_migration_guard->reset();
      *caller_migration_guard = std::move(*optional_caller_guard);
      return;
    }
    get_runtime()->detach();
    callee_migration_guard->reset();

    RuntimeSlabGuard slab_guard;
    RPCReturnBuffer ret_val_buf;
    *caller_migration_guard = Migrator::migrate_thread_and_ret_val<void>(
        std::move(ret_val_buf), to_proclet_id(caller_header), nullptr, nullptr);
    return;
  }
}

}  // namespace nu

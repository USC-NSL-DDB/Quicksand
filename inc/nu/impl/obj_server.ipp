#include <alloca.h>
#include <memory>
#include <type_traits>
#include <utility>
#include <alloca.h>

#include <net.h>
#include <sync.h>
#include <thread.h>

#include "nu/ctrl.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/migrator.hpp"
#include "nu/runtime.hpp"

namespace nu {

template <typename Cls, typename... As>
void ObjServer::construct_obj(cereal::BinaryInputArchive &ia,
                              RPCReturner *returner) {
  void *base;
  bool pinned;
  ia >> base >> pinned;

  Runtime::heap_manager->allocate(base, /* migratable = */ !pinned);

  auto *heap_header = reinterpret_cast<HeapHeader *>(base);
  heap_header->cpu_load.reset();
  auto &slab = heap_header->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  std::tuple<std::decay_t<As>...> args;
  std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);
  std::apply(
      [&](auto &&... args) {
        ObjSlabGuard obj_slab_guard(&slab);
        heap_header->status = kPresent;
        thread_set_owner_heap(thread_self(), base);
        new (obj_space) Cls(std::forward<As>(args)...);
        thread_unset_owner_heap();
      },
      args);

  Runtime::heap_manager->insert(base);

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp_ok(oa_sstream, returner);
}

template <typename Cls, typename... As>
void ObjServer::construct_obj_locally(void *base, bool pinned, As &&... args) {
  RuntimeSlabGuard runtime_slab_guard;
  Runtime::heap_manager->allocate(base, /* migratable = */ !pinned);

  auto *heap_header = reinterpret_cast<HeapHeader *>(base);
  heap_header->cpu_load.reset();
  auto &slab = heap_header->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  {
    ObjSlabGuard obj_slab_guard(&slab);
    heap_header->status = kPresent;
    thread_set_owner_heap(thread_self(), base);
    new (obj_space) Cls(std::forward<As>(args)...);
    thread_unset_owner_heap();
  }

  Runtime::heap_manager->insert(base);
}

template <typename Cls>
void ObjServer::__update_ref_cnt(Cls &obj, RPCReturner returner,
                                 HeapHeader *heap_header, int delta,
                                 bool *deallocate) {
  heap_header->spin.Lock();
  auto latest_cnt = (heap_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  heap_header->spin.Unlock();

  if (latest_cnt == 0) {
    if (likely(Runtime::heap_manager->remove_for_destruction(heap_header))) {
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
        HeapManager::wait_until_present(heap_header);
      }
      // Safe without acquiring the lock since the obj is dead now.
      heap_header->ref_cnt = -delta;
      RuntimeSlabGuard guard;
      send_rpc_resp_wrong_client(&returner);
      return;
    }
  }

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp_ok(oa_sstream, &returner);
}

template <typename Cls>
void ObjServer::update_ref_cnt(cereal::BinaryInputArchive &ia,
                               RPCReturner *returner) {
  RemObjID id;
  ia >> id;
  int delta;
  ia >> delta;

  auto *heap_base = to_heap_base(id);
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);

  bool deallocate = false;
  bool heap_not_found = !Runtime::run_within_obj_env<Cls>(
      heap_base, __update_ref_cnt<Cls>, *returner, heap_header, delta,
      &deallocate);

  if (deallocate) {
    // Wait for all ongoing invocations to finish.
    heap_header->rcu_lock.writer_sync();
    Runtime::heap_manager->deallocate(heap_base);
  }

  if (heap_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls>
bool ObjServer::update_ref_cnt_locally(RemObjID id, int delta) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(to_heap_base(id));
  heap_header->spin.Lock();
  auto latest_cnt = (heap_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  heap_header->spin.Unlock();

  if (latest_cnt == 0) {
    if (unlikely(!Runtime::heap_manager->remove_for_destruction(heap_header))) {
      return false;
    }
    // Won't be migrated at this point.
    {
      auto *obj = Runtime::get_obj<Cls>(id);
      ObjSlabGuard obj_slab_guard(&heap_header->slab);
      obj->~Cls();
    }

    RuntimeSlabGuard runtime_slab_guard;
    Runtime::heap_manager->deallocate(heap_header);
  }

  return true;
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::__run_closure(Cls &obj, HeapHeader *heap_header,
                              cereal::BinaryInputArchive &ia,
                              RPCReturner returner) {
  auto state = heap_header->cpu_load.monitor_start();
  heap_header->thread_cnt.inc_unsafe();

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
      std::apply(
          [&](auto &&... states) { fn(obj, std::forward<S1s>(states)...); },
          *states);
      std::destroy_at(states);
    }
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
  } else {
    MigrationEnabledGuard guard;
    auto ret = std::apply(
        [&](auto &&... states) {
          return fn(obj, std::forward<S1s>(states)...);
        },
        *states);
    std::destroy_at(states);
    guard.reset();

    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    oa_sstream->oa << ret;
  }

  send_rpc_resp_ok(oa_sstream, &returner);
  heap_header->thread_cnt.dec_unsafe();
  heap_header->cpu_load.monitor_end(state);
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::run_closure(cereal::BinaryInputArchive &ia,
                            RPCReturner *returner) {
  RemObjID id;
  ia >> id;
  auto *heap_header = to_heap_header(id);
  bool heap_not_found = !Runtime::run_within_obj_env<Cls>(
      heap_header, __run_closure<Cls, RetT, FnPtr, S1s...>, heap_header, ia,
      *returner);
  if (heap_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::run_closure_locally(RetT *caller_ptr, RemObjID caller_id,
                                    RemObjID callee_id, FnPtr fn_ptr,
                                    S1s &&... states) {
  auto *callee_heap_header = to_heap_header(callee_id);
  auto *caller_heap_header = to_heap_header(caller_id);
  auto state = callee_heap_header->cpu_load.monitor_start();
  callee_heap_header->thread_cnt.inc_unsafe();

  auto *obj = Runtime::get_obj<Cls>(callee_id);
  if constexpr (!std::is_same<RetT, void>::value) {
    auto *ret = reinterpret_cast<RetT *>(alloca(sizeof(RetT)));
    std::construct_at(ret);
    *ret = fn_ptr(*obj, std::forward<S1s>(states)...);
    callee_heap_header->thread_cnt.dec_unsafe();
    callee_heap_header->cpu_load.monitor_end(state);

    {
      NonBlockingMigrationDisabledGuard caller_guard(caller_heap_header);

      if (likely(caller_guard)) {
        {
          ObjSlabGuard caller_slab_guard(&caller_heap_header->slab);
          if constexpr (std::is_copy_constructible<RetT>::value) {
            // Perform a copy to ensure that the return value is allocated from
            // the caller heap. It must be a "deep copy"; for now we just assume
            // it is.
            *caller_ptr = *ret;
          } else {
            // Actually we should use ser/deser here.
            *caller_ptr = std::move(*ret);
          }
        }
        thread_set_owner_heap(thread_self(), caller_heap_header);
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
          // callee_heap_header->migrated_wg.Done();
          // }
          Runtime::archive_pool->put_oa_sstream(oa_sstream);
        });
  } else {
    fn_ptr(*obj, std::forward<S1s>(states)...);
    callee_heap_header->thread_cnt.dec_unsafe();
    callee_heap_header->cpu_load.monitor_end(state);

    {
      NonBlockingMigrationDisabledGuard caller_guard(caller_heap_header);

      if (likely(caller_guard)) {
        thread_set_owner_heap(thread_self(), caller_heap_header);
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
          //   callee_heap_header->migrated_wg.Done();
          // }
        });
  }
}

} // namespace nu

#include <type_traits>
#include <utility>

extern "C" {
#include <runtime/preempt.h>
}
#include <net.h>
#include <thread.h>

#include "nu/ctrl.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/migrator.hpp"
#include "nu/runtime.hpp"

namespace nu {

inline netaddr ObjServer::get_addr() const {
  netaddr addr = {.ip = get_cfg_ip(), .port = kObjServerPort};
  return addr;
}

template <typename Cls, typename... As>
void ObjServer::construct_obj(cereal::BinaryInputArchive &ia,
                              RPCReturner *returner) {
  void *base;
  bool pinned;
  ia >> base >> pinned;

  Runtime::heap_manager->allocate(base, /* migratable = */ !pinned);

  auto &slab = reinterpret_cast<HeapHeader *>(base)->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  std::tuple<std::decay_t<As>...> args;
  std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);
  std::apply(
      [&](auto &&... args) {
        Runtime::switch_to_obj_heap(obj_space);
        new (obj_space) Cls(std::forward<As>(args)...);
        Runtime::switch_to_runtime_heap();
      },
      args);

  barrier();
  Runtime::heap_manager->insert(base);

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp_ok(oa_sstream, returner);
}

template <typename Cls, typename... As>
void ObjServer::construct_obj_locally(void *base, bool pinned, As &&... args) {
  {
    RuntimeHeapGuard runtime_heap_guard;
    Runtime::heap_manager->allocate(base, /* migratable = */ !pinned);

    auto &slab = reinterpret_cast<HeapHeader *>(base)->slab;
    auto obj_space = slab.yield(sizeof(Cls));

    {
      ObjHeapGuard obj_heap_guard(obj_space);
      new (obj_space) Cls(std::forward<As>(args)...);
    }
  }

  barrier();
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
    if (likely(Runtime::heap_manager->remove(heap_header))) {
      *deallocate = true;
      obj.~Cls();
    } else {
      {
        MigrationEnabledGuard guard;
        while (!thread_is_migrated()) {
          rt::Yield();
        }
      }

      // Safe without acquiring the lock since the obj is dead now.
      heap_header->ref_cnt = -delta;
      RuntimeHeapGuard guard;
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
    Runtime::heap_manager->deallocate(heap_base);
  }

  if (heap_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls>
void ObjServer::update_ref_cnt_locally(RemObjID id, int delta) {
  auto *heap_header = reinterpret_cast<HeapHeader *>(to_heap_base(id));
  heap_header->spin.Lock();
  auto latest_cnt = (heap_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  heap_header->spin.Unlock();

  if (latest_cnt == 0) {
    auto *obj = Runtime::get_obj<Cls>(id);
    ObjHeapGuard obj_heap_guard(obj);
    obj->~Cls();
    {
      RuntimeHeapGuard runtime_heap_guard;
      Runtime::heap_manager->deallocate(heap_header);
    }
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::__run_closure(Cls &obj, cereal::BinaryInputArchive &ia,
                              RPCReturner returner) {
  decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;

  FnPtr fn;
  ia >> fn;

  std::tuple<std::decay_t<S1s>...> states;
  std::apply([&](auto &&... states) { ((ia >> states), ...); }, states);

  if constexpr (std::is_same<RetT, void>::value) {
    {
      MigrationEnabledGuard guard;
      std::apply(
          [&](auto &&... states) { fn(obj, std::forward<S1s>(states)...); },
          states);
    }
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
  } else {
    MigrationEnabledGuard guard;
    auto ret = std::apply(
        [&](auto &&... states) {
          return fn(obj, std::forward<S1s>(states)...);
        },
        states);
    guard.reset();

    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    oa_sstream->oa << ret;
  }

  send_rpc_resp_ok(oa_sstream, &returner);
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::run_closure(cereal::BinaryInputArchive &ia,
                            RPCReturner *returner) {
  RemObjID id;
  ia >> id;
  auto *heap_base = to_heap_base(id);

  bool heap_not_found = !Runtime::run_within_obj_env<Cls>(
      heap_base, __run_closure<Cls, RetT, FnPtr, S1s...>, ia, *returner);
  if (heap_not_found) {
    send_rpc_resp_wrong_client(returner);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
RetT ObjServer::run_closure_locally(RemObjID id, FnPtr fn_ptr,
                                    S1s &&... states) {
  auto *obj = Runtime::get_obj<Cls>(id);
  if constexpr (!std::is_same<RetT, void>::value) {
    RetT ret;
    {
      ObjHeapGuard guard(obj);
      ret = fn_ptr(*obj, std::forward<S1s>(states)...);
    }
    // Perform a copy to ensure that the return value is allocated from
    // the caller heap. It must be a "deep copy"; for now we just assume
    // it is.
    if constexpr (std::is_copy_constructible<RetT>::value) {
      auto ret_copy = ret;
      return ret_copy;
    } else {
      return ret;
    }
  } else {
    ObjHeapGuard guard(obj);
    fn_ptr(*obj, std::forward<S1s>(states)...);
  }
}

} // namespace nu

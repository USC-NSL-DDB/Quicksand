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
                              rt::TcpConn *rpc_conn) {
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
  send_rpc_resp(oa_sstream->ss, rpc_conn);
  Runtime::archive_pool->put_oa_sstream(oa_sstream);
}

template <typename Cls, typename... As>
void ObjServer::construct_obj_locally(void *base, bool pinned, As &&... args) {
  auto *heap = Runtime::get_heap();
  Runtime::switch_to_runtime_heap();
  Runtime::heap_manager->allocate(base, /* migratable = */ !pinned);

  auto &slab = reinterpret_cast<HeapHeader *>(base)->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  Runtime::switch_to_obj_heap(obj_space);
  new (obj_space) Cls(std::forward<As>(args)...);
  Runtime::set_heap(heap);

  barrier();
  Runtime::heap_manager->insert(base);
}

template <typename Cls>
void ObjServer::__update_ref_cnt(Cls &obj, rt::TcpConn *rpc_conn,
                                 HeapHeader *heap_header, int delta,
                                 bool *deallocate) {
  heap_header->spin.Lock();
  auto latest_cnt = (heap_header->ref_cnt += delta);
  BUG_ON(latest_cnt < 0);
  heap_header->spin.Unlock();

  if (latest_cnt == 0) {
  retry:
    if (likely(Runtime::heap_manager->remove_if_not_migrating(heap_header))) {
      *deallocate = true;
      obj.~Cls();
    } else {
      Runtime::migration_enable();
      while (ACCESS_ONCE(heap_header->migrating)) {
        rt::Yield();
      }
      Runtime::migration_disable();
      goto retry;
    }
  }

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  send_rpc_resp(oa_sstream->ss, rpc_conn);
  Runtime::archive_pool->put_oa_sstream(oa_sstream);
}

template <typename Cls>
void ObjServer::update_ref_cnt(cereal::BinaryInputArchive &ia,
                               rt::TcpConn *rpc_conn) {
  RemObjID id;
  ia >> id;
  int delta;
  ia >> delta;

  auto *heap_base = to_heap_base(id);
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);

  bool deallocate = false;
  bool client_retry = !Runtime::run_within_obj_env<Cls>(
      heap_base, __update_ref_cnt<Cls>, rpc_conn, heap_header, delta,
      &deallocate);

  if (unlikely(client_retry)) {
    send_rpc_client_retry(rpc_conn);
    return;
  }

  if (deallocate) {
    Runtime::heap_manager->deallocate(heap_base);
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
    auto *heap = Runtime::get_heap();
    auto *obj = Runtime::get_obj<Cls>(id);
    Runtime::switch_to_obj_heap(obj);
    obj->~Cls();
    Runtime::switch_to_runtime_heap();
    Runtime::heap_manager->deallocate(heap_header);
    Runtime::set_heap(heap);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::__run_closure(Cls &obj, cereal::BinaryInputArchive &ia,
                              rt::TcpConn *rpc_conn) {
  decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;

  FnPtr fn;
  ia >> fn;

  std::tuple<std::decay_t<S1s>...> states;
  std::apply([&](auto &&... states) { ((ia >> states), ...); }, states);

  Runtime::migration_enable();
  if constexpr (std::is_same<RetT, void>::value) {
    std::apply(
        [&](auto &&... states) { fn(obj, std::forward<S1s>(states)...); },
        states);
    Runtime::migration_disable();
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
  } else {
    auto ret = std::apply(
        [&](auto &&... states) {
          return fn(obj, std::forward<S1s>(states)...);
        },
        states);
    Runtime::migration_disable();
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    oa_sstream->oa << ret;
  }

  send_rpc_resp(oa_sstream->ss, rpc_conn);
  Runtime::archive_pool->put_oa_sstream(oa_sstream);
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::run_closure(cereal::BinaryInputArchive &ia,
                            rt::TcpConn *rpc_conn) {
  RemObjID id;
  ia >> id;
  auto *heap_base = to_heap_base(id);

  bool client_retry = !Runtime::run_within_obj_env<Cls>(
      heap_base, __run_closure<Cls, RetT, FnPtr, S1s...>, ia, rpc_conn);

  if (unlikely(client_retry)) {
    send_rpc_client_retry(rpc_conn);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
RetT ObjServer::run_closure_locally(RemObjID id, FnPtr fn_ptr,
                                    S1s &&... states) {
  auto &obj = *Runtime::get_obj<Cls>(id);
  auto *heap = Runtime::get_heap();
  Runtime::switch_to_obj_heap(&obj);
  if constexpr (!std::is_same<RetT, void>::value) {
    auto ret = fn_ptr(obj, std::forward<S1s>(states)...);
    Runtime::set_heap(heap);
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
    fn_ptr(obj, std::forward<S1s>(states)...);
    Runtime::set_heap(heap);
  }
}

inline void ObjServer::send_rpc_client_retry(rt::TcpConn *rpc_conn) {
  ObjRPCRespHdr hdr;
  hdr.rc = CLIENT_RETRY;
  hdr.payload_size = 0;
  BUG_ON(rpc_conn->WriteFull(&hdr, sizeof(hdr)) < 0);
}

void ObjServer::send_rpc_resp(auto &ss, rt::TcpConn *rpc_conn) {
  ObjRPCRespHdr hdr;
  auto view = ss.view();
  hdr.payload_size = ss.tellp();

  if (unlikely(thread_is_migrated())) {
    hdr.rc = FORWARDED;
    auto stack_top = get_obj_stack_range(thread_self()).end;
    Runtime::migrator->forward_to_original_server(rpc_conn, stack_top, hdr,
                                                  view.data());
  } else {
    hdr.rc = OK;
    if (hdr.payload_size) {
      const iovec iovecs[] = {
          {&hdr, sizeof(hdr)},
          {const_cast<char *>(view.data()), hdr.payload_size}};
      BUG_ON(rpc_conn->WritevFull(std::span(iovecs)) < 0);
    } else {
      BUG_ON(rpc_conn->WriteFull(&hdr, sizeof(hdr)) < 0);
    }
  }
}

} // namespace nu

#include <type_traits>
#include <utility>

extern "C" {
#include <runtime/preempt.h>
}
#include <net.h>
#include <thread.h>

#include "ctrl.hpp"
#include "heap_mgr.hpp"
#include "migrator.hpp"
#include "runtime.hpp"

namespace nu {

inline netaddr ObjServer::get_addr() const {
  netaddr addr = {.ip = get_cfg_ip(), .port = port_};
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
      [&](const As &... args) {
        Runtime::switch_to_obj_heap(obj_space);
        new (obj_space) Cls(args...);
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
void ObjServer::construct_obj_locally(void *base, bool pinned, As &... args) {
  Runtime::heap_manager->allocate(base, /* migratable = */ !pinned);

  auto &slab = reinterpret_cast<HeapHeader *>(base)->slab;
  auto obj_space = slab.yield(sizeof(Cls));

  new (obj_space) Cls(args...);
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
    *deallocate = true;
    obj.~Cls();
    if (unlikely(!Runtime::heap_manager->remove(heap_header))) {
      while (unlikely(thread_is_migrating())) {
        rt::Yield();
      }
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
    Runtime::get_obj<Cls>(id)->~Cls();
    Runtime::heap_manager->deallocate(heap_header);
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
    std::apply([&](auto &&... states) { fn(obj, states...); }, states);
    Runtime::migration_disable();
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
  } else {
    auto ret = std::apply([&](auto &&... states) { return fn(obj, states...); },
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
                                    S1s &... states) {
  auto &obj = *Runtime::get_obj<Cls>(id);
  if constexpr (!std::is_same<RetT, void>::value) {
    return fn_ptr(obj, states...);
  } else {
    fn_ptr(obj, states...);
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
    Runtime::migrator->forward_to_original_server(hdr, view.data(), rpc_conn);
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

#include <type_traits>
#include <utility>

extern "C" {
#include <runtime/preempt.h>
}

#include "ctrl.hpp"
#include "heap_mgr.hpp"
#include "migrator.hpp"
#include "runtime.hpp"
#include "utils/tcp.hpp"

namespace nu {

template <typename Cls, typename... As>
void ObjServer::construct_obj(cereal::BinaryInputArchive &ia,
                              tcpconn_t *rpc_conn) {
  std::stringstream ret_ss;
  void *base;
  ia >> base;

  Runtime::heap_manager->allocate(base);
  reinterpret_cast<HeapHeader *>(base)->ref_cnt = 1;

  auto *slab = Runtime::heap_manager->get_slab(base);
  auto obj_space = slab->allocate(sizeof(Cls));

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

  send_rpc_resp(ret_ss, rpc_conn);
}

template <typename Cls>
void ObjServer::__update_ref_cnt(Cls &obj, tcpconn_t *rpc_conn,
                                 HeapHeader *heap_header, int delta,
                                 bool *deallocate) {
  heap_header->spin.Lock();
  auto latest_cnt = (heap_header->ref_cnt += delta);
  heap_header->spin.Unlock();

  if (latest_cnt == 0) {
    *deallocate = true;
    obj.~Cls();
    if (unlikely(!Runtime::heap_manager->remove(heap_header))) {
      while (unlikely(!thread_is_migrated())) {
        thread_yield();
      }
    }
  }

  std::stringstream ret_ss;
  send_rpc_resp(ret_ss, rpc_conn);
}

template <typename Cls>
void ObjServer::update_ref_cnt(cereal::BinaryInputArchive &ia,
                               tcpconn_t *rpc_conn) {
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
    Runtime::heap_manager->rcu_synchronize();
    Runtime::heap_manager->deallocate(heap_base);
  }
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::__closure_handler(Cls &obj, cereal::BinaryInputArchive &ia,
                                  tcpconn_t *rpc_conn) {
  std::stringstream ret_ss;
  cereal::BinaryOutputArchive oa(ret_ss);

  FnPtr fn;
  ia >> fn;

  std::tuple<std::decay_t<S1s>...> states;
  std::apply([&](auto &&... states) { ((ia >> states), ...); }, states);

  Runtime::migration_enable();
  if constexpr (std::is_same<RetT, void>::value) {
    std::apply([&](auto &&... states) { fn(obj, states...); }, states);
  } else {
    auto ret = std::apply([&](auto &&... states) { return fn(obj, states...); },
                          states);
    oa << ret;
  }
  Runtime::migration_disable();

  send_rpc_resp(ret_ss, rpc_conn);
}

template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
void ObjServer::closure_handler(cereal::BinaryInputArchive &ia,
                                tcpconn_t *rpc_conn) {
  RemObjID id;
  ia >> id;
  auto *heap_base = to_heap_base(id);

  bool client_retry = !Runtime::run_within_obj_env<Cls>(
      heap_base, __closure_handler<Cls, RetT, FnPtr, S1s...>, ia, rpc_conn);

  if (unlikely(client_retry)) {
    send_rpc_client_retry(rpc_conn);
  }
}

template <typename Cls, typename RetT, typename MdPtr, typename... A1s>
void ObjServer::__method_handler(Cls &obj, cereal::BinaryInputArchive &ia,
                                 tcpconn_t *rpc_conn) {
  std::stringstream ret_ss;
  cereal::BinaryOutputArchive oa(ret_ss);

  MdPtr md;
  ia >> md.raw;

  std::tuple<std::decay_t<A1s>...> args;
  std::apply([&](auto &&... args) { ((ia >> args), ...); }, args);

  Runtime::migration_enable();
  if constexpr (std::is_same<RetT, void>::value) {
    std::apply([&](auto &&... args) { (obj.*(md.ptr))(args...); }, args);
  } else {
    auto ret = std::apply(
        [&](auto &&... args) { return (obj.*(md.ptr))(args...); }, args);
    oa << ret;
  }
  Runtime::migration_disable();

  send_rpc_resp(ret_ss, rpc_conn);
}

template <typename Cls, typename RetT, typename MdPtr, typename... A1s>
void ObjServer::method_handler(cereal::BinaryInputArchive &ia,
                               tcpconn_t *rpc_conn) {
  RemObjID id;
  ia >> id;
  auto *heap_base = to_heap_base(id);

  bool client_retry = !Runtime::run_within_obj_env<Cls>(
      heap_base, __method_handler<Cls, RetT, MdPtr, A1s...>, ia, rpc_conn);

  if (unlikely(client_retry)) {
    send_rpc_client_retry(rpc_conn);
  }
}

inline void ObjServer::send_rpc_client_retry(tcpconn_t *rpc_conn) {
  ObjRPCRespHdr hdr;
  hdr.rc = CLIENT_RETRY;
  hdr.payload_size = 0;
  tcp_write_until(rpc_conn, &hdr, sizeof(hdr));
}

} // namespace nu

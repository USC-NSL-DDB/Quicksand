#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
}
#include "thread.h"

#include "ctrl_client.hpp"
#include "obj_conn_mgr.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"
#include "runtime_deleter.hpp"
#include "utils/future.hpp"
#include "utils/promise.hpp"
#include "utils/tcp.hpp"

namespace nu {

template <typename... S1s>
void serialize(std::stringstream *output, S1s &&... states) {
  cereal::BinaryOutputArchive oa(*output);
  ((oa << std::forward<S1s>(states)), ...);
}

template <typename T>
tcpconn_t *RemObj<T>::purge_old_conns(RemObjID id, tcpconn_t *old_conn) {
  tcpconn_t *conn;
  auto old_srv_addr = tcp_remote_addr(old_conn);
  do {
    BUG_ON(tcp_shutdown(old_conn, SHUT_RDWR) < 0);
    tcp_close(old_conn);
    conn = Runtime::rem_obj_conn_mgr->get_conn(id);
    old_conn = conn;
  } while (tcp_remote_addr(conn) == old_srv_addr);
  return conn;
}

template <typename T>
template <typename RetT>
RetT RemObj<T>::invoke_remote(RemObjID id, const std::stringstream &states_ss) {
  tcpconn_t *conn;

  conn = Runtime::rem_obj_conn_mgr->get_conn(id);
retry:
  auto states_str = states_ss.str();
  uint64_t states_size = states_str.size();
  tcp_write2_until(conn, &states_size, sizeof(states_size), states_str.data(),
                   states_size);

  ObjRPCRespHdr hdr;
  tcp_read_until(conn, &hdr, sizeof(hdr));

  if (unlikely(hdr.rc == CLIENT_RETRY)) {
    conn = purge_old_conns(id, conn);
    goto retry;
  }

  std::string ret_str(hdr.payload_size, '\0');
  tcp_read_until(conn, ret_str.data(), hdr.payload_size);

  if (unlikely(hdr.rc == FORWARDED)) {
    conn = purge_old_conns(id, conn);
  }

  Runtime::rem_obj_conn_mgr->put_conn(id, conn);

  if constexpr (!std::is_same<RetT, void>::value) {
    std::stringstream ret_ss(std::move(ret_str));
    cereal::BinaryInputArchive ia(ret_ss);
    RetT ret;
    ia >> ret;
    return ret;
  }
}

template <typename T> RemObj<T>::RemObj(RemObjID id) : id_(id) {
  inc_ref_cnt();
}

template <typename T>
RemObj<T>::RemObj(RemObjID id, Future<void> &&construct)
    : id_(id), construct_(std::move(construct)) {}

template <typename T> RemObj<T>::~RemObj() {
  if (construct_) {
    construct_.get();
  }
  dec_ref_cnt();
}

template <typename T> RemObj<T>::RemObj(RemObj<T> &&o) : id_(o.id_) {}

template <typename T> RemObj<T> &RemObj<T>::operator=(RemObj<T> &&o) {
  id_ = o.id_;
  return *this;
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create(As &&... args) {
  auto optional = Runtime::controller_client->allocate_obj();
  BUG_ON(!optional);
  auto [id, range] = *optional;

  auto *args_ss = new std::stringstream();
  auto *handler = ObjServer::construct_obj<T, As...>;
  serialize(args_ss, handler, to_heap_base(id), std::forward<As>(args)...);

  auto *construct_promise = Promise<void>::create([args_ss, id]() {
    std::unique_ptr<std::stringstream> gc(args_ss);
    invoke_remote<void>(id, *args_ss);
  });
  return RemObj(id, std::move(construct_promise->get_future()));
}

template <typename T> RemObj<T> RemObj<T>::attach(Cap cap) {
  return RemObj<T>(cap.id);
}

template <typename T> RemObj<T>::Cap RemObj<T>::get_cap() const {
  Cap cap;
  cap.id = id_;
  return cap;
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemObj<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  using closure_states_checker[[maybe_unused]] =
      decltype(fn(std::declval<T &>(), states...));

  if (construct_) {
    construct_.get();
  }

  auto *states_ss = new std::stringstream();
  auto *handler = ObjServer::closure_handler<T, RetT, decltype(fn), S1s...>;
  serialize(states_ss, handler, id_, fn, std::forward<S1s>(states)...);
  auto *promise = Promise<RetT>::create([&, states_ss] {
    std::unique_ptr<std::stringstream> gc(states_ss);
    if constexpr (!std::is_same<RetT, void>::value) {
      return invoke_remote<RetT>(id_, *states_ss);
    } else {
      invoke_remote<void>(id_, *states_ss);
    }
  });
  return promise->get_future();
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemObj<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  return run_async(fn, std::forward<S1s>(states)...).get();
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
Future<RetT> RemObj<T>::run_async(RetT (T::*md)(A0s...), A1s &&... args) {
  using md_args_checker[[maybe_unused]] =
      decltype((std::declval<T>().*(md))(args...));

  if (construct_) {
    construct_.get();
  }

  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;

  auto *args_ss = new std::stringstream();
  auto *handler =
      ObjServer::method_handler<T, RetT, decltype(method_ptr), A1s...>;
  serialize(args_ss, handler, id_, method_ptr.raw, std::forward<A1s>(args)...);

  auto *promise = Promise<RetT>::create([&, args_ss] {
    std::unique_ptr<std::stringstream> gc(args_ss);
    if constexpr (!std::is_same<RetT, void>::value) {
      return invoke_remote<RetT>(id_, *args_ss);
    } else {
      invoke_remote<void>(id_, *args_ss);
    }
  });
  return promise->get_future();
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT RemObj<T>::run(RetT (T::*fn)(A0s...), A1s &&... args) {
  return run_async(fn, std::forward<A1s>(args)...).get();
};

template <typename T> void RemObj<T>::inc_ref_cnt() {
  auto future =
      update_ref_cnt(1)->template get_future<RuntimeDeleter<Promise<void>>>();
  Runtime::obj_inflight_inc_cnts->put(id_, std::move(future));
}

template <typename T> void RemObj<T>::dec_ref_cnt() {
  RuntimeFuture<void> inc_future;
  if (Runtime::obj_inflight_inc_cnts->try_get_and_remove(id_, &inc_future)) {
    inc_future.get();
  }
  auto *dec_promise = update_ref_cnt(-1);
  Runtime::rcu_lock.lock();
  rt::Thread([=]() {
    dec_promise->template get_future<RuntimeDeleter<Promise<void>>>().get();
    Runtime::rcu_lock.unlock();
  })
      .Detach();
}

template <typename T> Promise<void> *RemObj<T>::update_ref_cnt(int delta) {
  auto *args_ss = new std::stringstream();
  auto *handler = ObjServer::update_ref_cnt<T>;
  serialize(args_ss, handler, id_, delta);
  return Promise<void>::create<RuntimeAllocator<Promise<void>>>(
      [&, args_ss, id = id_] {
        std::unique_ptr<std::stringstream> gc(args_ss);
        invoke_remote<void>(id, *args_ss);
      });
}
} // namespace nu

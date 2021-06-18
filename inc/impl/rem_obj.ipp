#include <array>
#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
#include <runtime/net.h>
}
#include <thread.h>

#include "ctrl_client.hpp"
#include "exception.hpp"
#include "obj_conn_mgr.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "utils/future.hpp"
#include "utils/netaddr.hpp"
#include "utils/promise.hpp"
#include "utils/type_traits.hpp"

namespace nu {

template <typename... S1s>
void serialize(cereal::BinaryOutputArchive *oa, S1s &&... states) {
  ((*oa << std::forward<S1s>(states)), ...);
}

template <typename T>
template <typename RetT>
RetT RemObj<T>::invoke_remote(RemObjID id, auto *states_ss) {
retry:
  auto conn = Runtime::rem_obj_conn_mgr->get_conn(id);
  auto states_view = states_ss->view();
  uint64_t states_size = states_ss->tellp();

  const iovec iovecs[] = {
      {&states_size, sizeof(states_size)},
      {const_cast<char *>(states_view.data()), states_size}};
  BUG_ON(conn->WritevFull(std::span(iovecs)) < 0);

  ObjRPCRespHdr hdr;
  BUG_ON(conn->ReadFull(&hdr, sizeof(hdr)) <= 0);

  if (unlikely(hdr.rc == CLIENT_RETRY)) {
    Runtime::rem_obj_conn_mgr->update_addr(id);
    Runtime::rem_obj_conn_mgr->put_conn(conn);
    goto retry;
  } else if (unlikely(hdr.rc == FORWARDED)) {
    Runtime::rem_obj_conn_mgr->update_addr(id);
  }

  auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
  auto &[ret_ss, ia] = *ia_sstream;
  if (unlikely(ret_ss.view().size() < hdr.payload_size)) {
    ret_ss.str(std::string(hdr.payload_size, '\0'));
  }
  BUG_ON(conn->ReadFull(const_cast<char *>(ret_ss.view().data()),
                        hdr.payload_size) < 0);

  Runtime::rem_obj_conn_mgr->put_conn(conn);

  if constexpr (!std::is_same<RetT, void>::value) {
    RetT ret;
    ia >> ret;
    Runtime::archive_pool->put_ia_sstream(ia_sstream);
    return ret;
  } else {
    Runtime::archive_pool->put_ia_sstream(ia_sstream);
  }
}

template <typename T>
RemObj<T>::RemObj(RemObjID id, bool ref_cnted)
    : id_(id), ref_cnted_(ref_cnted) {
  if (ref_cnted) {
    auto *inc_ref_promise = update_ref_cnt(1);
    if (inc_ref_promise) {
      inc_ref_ = std::move(inc_ref_promise->get_future());
    }
  }
}

template <typename T>
RemObj<T>::RemObj(const Cap &cap, bool ref_cnted) : RemObj(cap.id, ref_cnted) {}

template <typename T>
RemObj<T>::RemObj() : id_(kNullRemObjID), ref_cnted_(false) {}

template <typename T> RemObj<T>::~RemObj() { reset(); }

template <typename T>
RemObj<T>::RemObj(RemObj<T> &&o)
    : id_(o.id_), inc_ref_(std::move(o.inc_ref_)), ref_cnted_(o.ref_cnted_) {
  o.ref_cnted_ = false;
}

template <typename T> RemObj<T> &RemObj<T>::operator=(RemObj<T> &&o) {
  this->~RemObj();
  id_ = o.id_;
  inc_ref_ = std::move(o.inc_ref_);
  ref_cnted_ = o.ref_cnted_;
  o.ref_cnted_ = false;
  return *this;
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create(As &&... args) {
  return general_create(/* pinned = */ false, std::nullopt,
                        std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
Future<RemObj<T>> RemObj<T>::create_async(As &&... args) {
  auto *promise = Promise<RemObj<T>>::create([&, args...]() mutable {
    return general_create(/* pinned = */ false, std::nullopt,
                          std::forward<As>(args)...);
  });
  return promise->get_future();
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create_at(netaddr addr, As &&... args) {
  return general_create(/* pinned = */ false, addr, std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
Future<RemObj<T>> RemObj<T>::create_at_async(netaddr addr, As &&... args) {
  auto *promise = Promise<RemObj<T>>::create([&, addr, args...]() mutable {
    return general_create(/* pinned = */ false, addr,
                          std::forward<As>(args)...);
  });
  return promise->get_future();
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create_pinned(As &&... args) {
  return general_create(/* pinned = */ true, std::nullopt,
                        std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
Future<RemObj<T>> RemObj<T>::create_pinned_async(As &&... args) {
  auto *promise = Promise<RemObj<T>>::create([&, args...]() mutable {
    return general_create(/* pinned = */ true, std::nullopt,
                          std::forward<As>(args)...);
  });
  return promise->get_future();
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create_pinned_at(netaddr addr, As &&... args) {
  return general_create(/* pinned = */ true, addr, std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
Future<RemObj<T>> RemObj<T>::create_pinned_at_async(netaddr addr,
                                                    As &&... args) {
  auto *promise = Promise<RemObj<T>>::create([&, addr, args...]() mutable {
    return general_create(/* pinned = */ true, addr, std::forward<As>(args)...);
  });
  return promise->get_future();
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::general_create(bool pinned, std::optional<netaddr> hint,
                                    As &&... args) {
  auto optional = Runtime::controller_client->allocate_obj(hint);
  if (unlikely(!optional)) {
    throw OutOfMemory();
  }
  auto [id, server_addr] = *optional;

  RemObj<T> rem_obj;
  rem_obj.id_ = id;
  rem_obj.ref_cnted_ = true;

  if (Runtime::obj_server && server_addr == Runtime::obj_server->get_addr()) {
    // Fast path: the heap is actually local, use normal function call.
    ObjServer::construct_obj_locally<T, As...>(to_heap_base(id), pinned,
                                               std::forward<As>(args)...);
    return rem_obj;
  }

  Runtime::migration_disable();

  // Slow path: the heap is actually remote, use RPC.
  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::construct_obj<T, As...>;
  serialize(&oa_sstream->oa, handler, to_heap_base(id), pinned,
            std::forward<As>(args)...);

  invoke_remote<void>(id, &oa_sstream->ss);
  Runtime::archive_pool->put_oa_sstream(oa_sstream);
  Runtime::migration_enable();

  return rem_obj;
}

template <typename T> RemObj<T>::Cap RemObj<T>::get_cap() const {
  Cap cap;
  cap.id = id_;
  return cap;
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemObj<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  assert_no_pointer_or_lval_ref<RetT, S0s...>();
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::forward<S1s>(states)...));

  return __run_async(fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemObj<T>::__run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  auto *promise = Promise<RetT>::create([&, fn, states...]() mutable {
    return __run(fn, std::forward<S1s>(states)...);
  });
  return promise->get_future();
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemObj<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  assert_no_pointer_or_lval_ref<RetT, S0s...>();
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::forward<S1s>(states)...));

  return __run(fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemObj<T>::__run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  Runtime::migration_disable();

  if (__is_local()) {
    // Fast path: the heap is actually local, use function call.
    auto *local_fn =
        ObjServer::run_closure_locally<T, RetT, decltype(fn), S1s...>;
    if constexpr (!std::is_same<RetT, void>::value) {
      auto ret = local_fn(id_, fn, std::forward<S1s>(states)...);
      Runtime::migration_enable();
      return ret;
    } else {
      local_fn(id_, fn, std::forward<S1s>(states)...);
      Runtime::migration_enable();
      return;
    }
  }

  // Slow path: the heap is actually remote, use RPC.
  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::run_closure<T, RetT, decltype(fn), S1s...>;
  serialize(&oa_sstream->oa, handler, id_, fn, std::forward<S1s>(states)...);

  if constexpr (!std::is_same<RetT, void>::value) {
    auto ret = invoke_remote<RetT>(id_, &oa_sstream->ss);
    Runtime::archive_pool->put_oa_sstream(oa_sstream);
    Runtime::migration_enable();
    return ret;
  } else {
    invoke_remote<void>(id_, &oa_sstream->ss);
    Runtime::archive_pool->put_oa_sstream(oa_sstream);
    Runtime::migration_enable();
  }
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
Future<RetT> RemObj<T>::run_async(RetT (T::*md)(A0s...), A1s &&... args) {
  assert_no_pointer_or_lval_ref<RetT, A0s...>();
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(std::forward<A1s>(args)...));

  return __run_async(md, std::forward<A1s>(args)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
Future<RetT> RemObj<T>::__run_async(RetT (T::*md)(A0s...), A1s &&... args) {
  auto *promise = Promise<RetT>::create([&, md, args...]() mutable {
    return __run(md, std::forward<A1s>(args)...);
  });
  return promise->get_future();
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT RemObj<T>::run(RetT (T::*md)(A0s...), A1s &&... args) {
  assert_no_pointer_or_lval_ref<RetT, A0s...>();
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(std::forward<A1s>(args)...));

  return __run(md, std::forward<A1s>(args)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT RemObj<T>::__run(RetT (T::*md)(A0s...), A1s &&... args) {
  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;
  return __run(
      +[](T &t, decltype(method_ptr) method_ptr, A1s &&... args) {
        return (t.*(method_ptr.ptr))(std::forward<A1s>(args)...);
      },
      method_ptr, std::forward<A1s>(args)...);
}

template <typename T> Promise<void> *RemObj<T>::update_ref_cnt(int delta) {
  Runtime::migration_disable();

  if (__is_local()) {
    // Fast path: the heap is actually local, use function call.
    ObjServer::update_ref_cnt_locally<T>(id_, delta);
    Runtime::migration_enable();
    return nullptr;
  }

  // Slow path: the heap is actually remote, use RPC.
  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::update_ref_cnt<T>;
  serialize(&oa_sstream->oa, handler, id_, delta);

  return Promise<void>::create([&, id = id_, oa_sstream] {
    invoke_remote<void>(id, &oa_sstream->ss);
    Runtime::archive_pool->put_oa_sstream(oa_sstream);
    Runtime::migration_enable();
  });
}

template <typename T> bool RemObj<T>::is_local() const {
  Runtime::migration_disable();
  bool ret = __is_local();
  Runtime::migration_enable();
  return ret;
}

template <typename T> bool RemObj<T>::__is_local() const {
  return Runtime::heap_manager &&
         Runtime::heap_manager->contains(to_heap_base(id_));
}

template <typename T> void RemObj<T>::reset() {
  if (ref_cnted_) {
    ref_cnted_ = false;
    if (inc_ref_) {
      inc_ref_.get();
    }

    auto *dec_promise = update_ref_cnt(-1);
    if (dec_promise) {
      dec_promise->get_future().get();
    }
  }
}

template <typename T> Future<void> RemObj<T>::reset_async() {
  if (ref_cnted_) {
    ref_cnted_ = false;
    if (inc_ref_) {
      inc_ref_.get();
    }

    auto *dec_promise = update_ref_cnt(-1);
    if (dec_promise) {
      return dec_promise->get_future();
    } else {
      return Promise<void>::create([] {})->get_future();
    }
  }
}

template <typename T> void RemObj<T>::reset_bg() {
  if (ref_cnted_) {
    ref_cnted_ = false;
    if (inc_ref_) {
      inc_ref_.get();
    }

    // Should allocate from the runtime slab, since the root object might be
    // destructed earlier than this background thread.
    auto *old_heap = Runtime::get_heap();
    Runtime::switch_to_runtime_heap();
    auto *dec_promise = update_ref_cnt(-1);
    if (dec_promise) {
      Runtime::rcu_lock.reader_lock();
      rt::Thread([=]() {
        dec_promise->get_future().get();
        Runtime::rcu_lock.reader_unlock();
      }).Detach();
    }
    Runtime::set_heap(old_heap);
  }
}

template <typename T>
template <class Archive>
void RemObj<T>::save(Archive &ar) const {
  const_cast<RemObj<T> *>(this)->ref_cnted_ = false;
  ar(id_);
}

template <typename T>
template <class Archive>
void RemObj<T>::load(Archive &ar) {
  ar(id_);
  ref_cnted_ = true;
}

} // namespace nu

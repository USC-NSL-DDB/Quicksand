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

#include "nu/ctrl_client.hpp"
#include "nu/exception.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/obj_server.hpp"
#include "nu/rpc_server.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/netaddr.hpp"
#include "nu/utils/promise.hpp"
#include "nu/utils/type_traits.hpp"

namespace nu {

template <typename... S1s> void serialize(auto *oa_sstream, S1s &&... states) {
  RuntimeSlabGuard slab_guard;

  auto &ss = oa_sstream->ss;
  auto *rpc_type = const_cast<RPCReqType *>(
      reinterpret_cast<const RPCReqType *>(ss.view().data()));
  *rpc_type = kRemObjCall;
  ss.seekp(sizeof(RPCReqType));

  auto &oa = oa_sstream->oa;
  ((oa << std::forward<S1s>(states)), ...);
}

template <typename T>
template <typename... S1s>
void RemObj<T>::invoke_remote(RemObjID id, S1s &&... states) {
  decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;
  HeapHeader *heap_header;

  {
    MigrationDisabledGuard disabled_guard;
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    serialize(oa_sstream, std::forward<S1s>(states)...);
    heap_header = reinterpret_cast<HeapHeader *>(thread_unset_owner_heap());
  }

retry:
  auto states_view = oa_sstream->ss.view();
  auto states_data = reinterpret_cast<const std::byte *>(states_view.data());
  auto states_size = oa_sstream->ss.tellp();

  NonBlockingMigrationDisabledGuard disabled_guard(nullptr);
  RPCReturnBuffer return_buf;
  RPCReturnCode rc;
  auto args_span = std::span(states_data, states_size);
  {
    RuntimeSlabGuard slab_guard;
    auto *client = Runtime::rpc_client_mgr->get_by_rem_obj_id(id);
    rc = client->Call(args_span, &return_buf);
    if (unlikely(rc == kErrWrongClient)) {
      Runtime::rpc_client_mgr->update_cache(id, client);
      goto retry;
    }
  }

  disabled_guard.reset(heap_header);
  if (heap_header && unlikely(!disabled_guard)) {
    Runtime::archive_pool->put_oa_sstream(oa_sstream);

    RuntimeSlabGuard slab_guard;
    Migrator::migrate_thread_and_ret_val<void>(
        std::move(return_buf), to_obj_id(heap_header), nullptr, nullptr);
    return;
  }

  Runtime::archive_pool->put_oa_sstream(oa_sstream);
  thread_set_owner_heap(thread_self(), heap_header);
}

template <typename T>
template <typename RetT, typename... S1s>
RetT RemObj<T>::invoke_remote_with_ret(RemObjID id, S1s &&... states) {
  decltype(Runtime::archive_pool->get_oa_sstream()) oa_sstream;
  HeapHeader *heap_header;
  RetT ret;

  {
    MigrationDisabledGuard disabled_guard;
    oa_sstream = Runtime::archive_pool->get_oa_sstream();
    serialize(oa_sstream, std::forward<S1s>(states)...);
    heap_header = reinterpret_cast<HeapHeader *>(thread_unset_owner_heap());
  }

retry:
  auto states_view = oa_sstream->ss.view();
  auto states_data = reinterpret_cast<const std::byte *>(states_view.data());
  auto states_size = oa_sstream->ss.tellp();

  NonBlockingMigrationDisabledGuard disabled_guard(nullptr);
  RPCReturnBuffer return_buf;
  RPCReturnCode rc;
  auto args_span = std::span(states_data, states_size);
  {
    RuntimeSlabGuard slab_guard;
    auto *client = Runtime::rpc_client_mgr->get_by_rem_obj_id(id);
    rc = client->Call(args_span, &return_buf);
    if (unlikely(rc == kErrWrongClient)) {
      Runtime::rpc_client_mgr->update_cache(id, client);
      goto retry;
    }
  }

  disabled_guard.reset(heap_header);
  if (heap_header && unlikely(!disabled_guard)) {
    Runtime::archive_pool->put_oa_sstream(oa_sstream);

    RuntimeSlabGuard slab_guard;
    Migrator::migrate_thread_and_ret_val<RetT>(
        std::move(return_buf), to_obj_id(heap_header), &ret, nullptr);
    return ret;
  }

  assert(rc == kOk);
  auto *ia_sstream = Runtime::archive_pool->get_ia_sstream();
  auto &[ret_ss, ia] = *ia_sstream;
  auto return_span = return_buf.get_mut_buf();
  ret_ss.span(
      {reinterpret_cast<char *>(return_span.data()), return_span.size()});
  ia >> ret;
  Runtime::archive_pool->put_ia_sstream(ia_sstream);

  Runtime::archive_pool->put_oa_sstream(oa_sstream);
  thread_set_owner_heap(thread_self(), heap_header);

  return ret;
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
  reset();
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
  return nu::async([&, ... args = std::forward<As>(args)]() {
    return general_create(/* pinned = */ false, std::nullopt,
                          std::forward<As>(args)...);
  });
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create_at(netaddr addr, As &&... args) {
  return general_create(/* pinned = */ false, addr, std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
Future<RemObj<T>> RemObj<T>::create_at_async(netaddr addr, As &&... args) {
  return nu::async([&, addr, ... args = std::forward<As>(args)]() {
    return general_create(/* pinned = */ false, addr,
                          std::forward<As>(args)...);
  });
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
  return nu::async([&, ... args = std::forward<As>(args)]() {
    return general_create(/* pinned = */ true, std::nullopt,
                          std::forward<As>(args)...);
  });
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
  return nu::async([&, addr, ... args = std::forward<As>(args)]() {
    return general_create(/* pinned = */ true, addr, std::forward<As>(args)...);
  });
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::general_create(bool pinned, std::optional<netaddr> hint,
                                    As &&... args) {
  RemObjID id;
  netaddr server_addr;
  HeapHeader *heap_header;

  {
    MigrationDisabledGuard disabled_guard;
    heap_header = reinterpret_cast<HeapHeader *>(thread_unset_owner_heap());
  }

  {
    RuntimeSlabGuard guard;
    auto optional = Runtime::controller_client->allocate_obj(hint);
    if (unlikely(!optional)) {
      throw OutOfMemory();
    }
    std::tie(id, server_addr) = *optional;

    NonBlockingMigrationDisabledGuard disabled_guard(heap_header);
    if (heap_header && unlikely(!disabled_guard)) {
      RPCReturnBuffer return_buf;
      Migrator::migrate_thread_and_ret_val<void>(
          std::move(return_buf), to_obj_id(heap_header), nullptr, nullptr);
    } else {
      thread_set_owner_heap(thread_self(), heap_header);
    }
  }

  RemObj<T> rem_obj;
  rem_obj.id_ = id;
  rem_obj.ref_cnted_ = true;

  {
    MigrationDisabledGuard disabled_guard;
    if (Runtime::obj_server && server_addr == Runtime::obj_server->get_addr()) {
      // Fast path: the heap is actually local, use normal function call.
      ObjServer::construct_obj_locally<T, As...>(to_heap_base(id), pinned,
                                                 std::forward<As>(args)...);
      return rem_obj;
    }
  }

  // Cold path: use RPC.
  auto *handler = ObjServer::construct_obj<T, As...>;
  invoke_remote(id, handler, to_heap_base(id), pinned,
                std::forward<As>(args)...);
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
  return nu::async([&, fn, ... states = std::forward<S1s>(states)]() mutable {
    return __run(fn, std::forward<S1s>(states)...);
  });
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
  auto *caller_heap_header = Runtime::get_current_obj_heap_header();

  if (caller_heap_header) {
    auto callee_heap_header = to_heap_header(id_);
    void *caller_slab = nullptr;
    {
      rt::Preempt p;
      rt::PreemptGuard g(&p);

      if (callee_heap_header->status == kPresent) {
        caller_slab = Runtime::switch_slab(&callee_heap_header->slab);
        thread_set_owner_heap(thread_self(), callee_heap_header);
      }
    }
    if (caller_slab) {
      // Fast path: the callee heap is actually local, use function call.
      if constexpr (!std::is_same<RetT, void>::value) {
        RetT ret;
        ObjServer::run_closure_locally<T, RetT, decltype(fn), S1s...>(
            &ret, to_obj_id(caller_heap_header), id_, fn,
            std::forward<S1s>(states)...);
        Runtime::switch_slab(caller_slab);
        return ret;
      } else {
        ObjServer::run_closure_locally<T, RetT, decltype(fn), S1s...>(
            nullptr, to_obj_id(caller_heap_header), id_, fn,
            std::forward<S1s>(states)...);
        Runtime::switch_slab(caller_slab);
        return;
      }
    }
  }

  // Slow path: the callee heap is actually remote, use RPC.
  auto *handler = ObjServer::run_closure<T, RetT, decltype(fn), S1s...>;
  if constexpr (!std::is_same<RetT, void>::value) {
    auto ret = invoke_remote_with_ret<RetT>(id_, handler, id_, fn,
                                            std::forward<S1s>(states)...);
    return ret;
  } else {
    invoke_remote(id_, handler, id_, fn, std::forward<S1s>(states)...);
  }
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemObj<T>::__run_and_get_loc(bool *is_local, RetT (*fn)(T &, S0s...),
                                  S1s &&... states) {
  auto *caller_heap_header = Runtime::get_current_obj_heap_header();

  if (caller_heap_header) {
    auto callee_heap_header = to_heap_header(id_);
    void *caller_slab = nullptr;
    {
      NonBlockingMigrationDisabledGuard callee_disabled_guard(
          callee_heap_header);
      if (callee_disabled_guard) {
        caller_slab = Runtime::switch_slab(&callee_heap_header->slab);
        thread_set_owner_heap(thread_self(), callee_heap_header);
      }
    }
    if (caller_slab) {
      *is_local = true;
      // Fast path: the callee heap is actually local, use function call.
      if constexpr (!std::is_same<RetT, void>::value) {
        RetT ret;
        ObjServer::run_closure_locally<T, RetT, decltype(fn), S1s...>(
            &ret, to_obj_id(caller_heap_header), id_, fn,
            std::forward<S1s>(states)...);
        Runtime::switch_slab(caller_slab);
        return ret;
      } else {
        ObjServer::run_closure_locally<T, RetT, decltype(fn), S1s...>(
            nullptr, to_obj_id(caller_heap_header), id_, fn,
            std::forward<S1s>(states)...);
        Runtime::switch_slab(caller_slab);
        return;
      }
    }
  }

  *is_local = false;
  // Slow path: the callee heap is actually remote, use RPC.
  auto *handler = ObjServer::run_closure<T, RetT, decltype(fn), S1s...>;
  if constexpr (!std::is_same<RetT, void>::value) {
    auto ret = invoke_remote_with_ret<RetT>(id_, handler, id_, fn,
                                            std::forward<S1s>(states)...);
    return ret;
  } else {
    invoke_remote(id_, handler, id_, fn, std::forward<S1s>(states)...);
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
  return nu::async([&, md, ... args = std::forward<A1s>(args)]() mutable {
    return __run(md, std::forward<A1s>(args)...);
  });
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT RemObj<T>::__run_and_get_loc(bool *is_local, RetT (T::*md)(A0s...),
                                  A1s &&... args) {
  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;
  return __run_and_get_loc(
      is_local,
      +[](T &t, decltype(method_ptr) method_ptr, A1s &&... args) {
        return (t.*(method_ptr.ptr))(std::forward<A1s>(args)...);
      },
      method_ptr, std::forward<A1s>(args)...);
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
  {
    rt::Preempt p;
    rt::PreemptGuard g(&p);

    auto *callee_heap_header = to_heap_header(id_);
    if (Runtime::obj_server && callee_heap_header->status == kPresent) {
      // Fast path: the heap is actually local, use function call.
      ObjServer::update_ref_cnt_locally<T>(id_, delta);
      return nullptr;
    }
  }

  // Slow path: the heap is actually remote, use RPC.
  auto *promise = Promise<void>::create([&, id = id_, delta]() {
    auto *handler = ObjServer::update_ref_cnt<T>;
    invoke_remote(id, handler, id, delta);
  });
  return promise;
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
      return nu::async([] {});
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
    RuntimeSlabGuard guard;
    auto *dec_promise = update_ref_cnt(-1);
    if (dec_promise) {
      Runtime::rcu_lock.reader_lock();
      rt::Thread([=]() {
        dec_promise->get_future().get();
        Runtime::rcu_lock.reader_unlock();
      }).Detach();
    }
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

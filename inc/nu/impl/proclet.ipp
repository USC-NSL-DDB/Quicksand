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
#include "nu/utils/type_traits.hpp"

namespace nu {

template <typename... S1s> void serialize(auto *oa_sstream, S1s &&... states) {
  RuntimeSlabGuard slab_guard;

  auto &ss = oa_sstream->ss;
  auto *rpc_type = const_cast<RPCReqType *>(
      reinterpret_cast<const RPCReqType *>(ss.view().data()));
  *rpc_type = kProcletCall;
  ss.seekp(sizeof(RPCReqType));

  auto &oa = oa_sstream->oa;
  ((oa << std::forward<S1s>(states)), ...);
}

template <typename T>
template <typename... S1s>
void Proclet<T>::invoke_remote(ProcletID id, S1s &&... states) {
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
    auto *client = Runtime::rpc_client_mgr->get_by_proclet_id(id);
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
RetT Proclet<T>::invoke_remote_with_ret(ProcletID id, S1s &&... states) {
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
    auto *client = Runtime::rpc_client_mgr->get_by_proclet_id(id);
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
Proclet<T>::Proclet(ProcletID id, bool ref_cnted)
    : id_(id), ref_cnted_(ref_cnted) {
  if (ref_cnted) {
    auto inc_ref_optional = update_ref_cnt(1);
    if (inc_ref_optional) {
      inc_ref_ = std::move(*inc_ref_optional);
    }
  }
}

template <typename T>
Proclet<T>::Proclet() : id_(kNullProcletID), ref_cnted_(false) {}

template <typename T> Proclet<T>::~Proclet() { reset(); }

template <typename T> Proclet<T>::Proclet(const Proclet<T> &o) : id_(o.id_) {
  auto inc_ref_optional = update_ref_cnt(1);
  if (inc_ref_optional) {
    inc_ref_optional->get();
  }
  ref_cnted_ = true;
}

template <typename T> Proclet<T> &Proclet<T>::operator=(const Proclet<T> &o) {
  reset();  
  id_ = o.id_;
  auto inc_ref_optional = update_ref_cnt(1);
  if (inc_ref_optional) {
    inc_ref_optional->get();
  }
  ref_cnted_ = true;
  return *this;
}

template <typename T>
Proclet<T>::Proclet(Proclet<T> &&o) noexcept
    : id_(o.id_), inc_ref_(std::move(o.inc_ref_)), ref_cnted_(o.ref_cnted_) {
  o.ref_cnted_ = false;
}

template <typename T> Proclet<T> &Proclet<T>::operator=(Proclet<T> &&o) noexcept {
  reset();
  id_ = o.id_;
  inc_ref_ = std::move(o.inc_ref_);
  ref_cnted_ = o.ref_cnted_;
  o.ref_cnted_ = false;
  return *this;
}

template <typename T>
template <typename... As>
Proclet<T> Proclet<T>::__create(bool pinned, uint32_t ip_hint,
                                    As &&... args) {
  ProcletID id;
  uint32_t server_ip;
  HeapHeader *heap_header;

  {
    MigrationDisabledGuard disabled_guard;
    heap_header = reinterpret_cast<HeapHeader *>(thread_unset_owner_heap());
  }

  {
    RuntimeSlabGuard guard;
    auto optional = Runtime::controller_client->allocate_obj(ip_hint);
    if (unlikely(!optional)) {
      throw OutOfMemory();
    }
    std::tie(id, server_ip) = *optional;

    NonBlockingMigrationDisabledGuard disabled_guard(heap_header);
    if (heap_header && unlikely(!disabled_guard)) {
      RPCReturnBuffer return_buf;
      Migrator::migrate_thread_and_ret_val<void>(
          std::move(return_buf), to_obj_id(heap_header), nullptr, nullptr);
    } else {
      thread_set_owner_heap(thread_self(), heap_header);
    }
  }

  Proclet<T> proclet;
  proclet.id_ = id;
  proclet.ref_cnted_ = true;

  {
    MigrationDisabledGuard disabled_guard;
    if (Runtime::rpc_server && server_ip == get_cfg_ip()) {
      // Fast path: the heap is actually local, use normal function call.
      ObjServer::construct_obj_locally<T, As...>(to_heap_base(id), pinned,
                                                 std::forward<As>(args)...);
      return proclet;
    }
  }

  // Cold path: use RPC.
  auto *handler = ObjServer::construct_obj<T, As...>;
  invoke_remote(id, handler, to_heap_base(id), pinned,
                std::forward<As>(args)...);
  return proclet;
}

template <typename T> ProcletID Proclet<T>::get_id() const { return id_; }

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> Proclet<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  assert_no_pointer_or_lval_ref<RetT, S0s...>();
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::move(states)...));

  return __run_async(fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> Proclet<T>::__run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  return nu::async([&, fn, ... states = std::forward<S1s>(states)]() mutable {
    return __run(fn, std::forward<S1s>(states)...);
  });
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT Proclet<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  assert_no_pointer_or_lval_ref<RetT, S0s...>();
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::move(states)...));

  return __run(fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT Proclet<T>::__run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  auto *caller_heap_header = Runtime::get_current_obj_heap_header();

  if (caller_heap_header) {
    auto callee_heap_header = to_heap_header(id_);
    void *caller_slab = nullptr;
    {
      NonBlockingMigrationDisabledGuard callee_guard(callee_heap_header);

      if (callee_guard) {
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
RetT Proclet<T>::__run_and_get_loc(bool *is_local, RetT (*fn)(T &, S0s...),
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
Future<RetT> Proclet<T>::run_async(RetT (T::*md)(A0s...), A1s &&... args) {
  assert_no_pointer_or_lval_ref<RetT, A0s...>();
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(std::move(args)...));

  return __run_async(md, std::forward<A1s>(args)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
Future<RetT> Proclet<T>::__run_async(RetT (T::*md)(A0s...), A1s &&... args) {
  return nu::async([&, md, ... args = std::forward<A1s>(args)]() mutable {
    return __run(md, std::forward<A1s>(args)...);
  });
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT Proclet<T>::__run_and_get_loc(bool *is_local, RetT (T::*md)(A0s...),
                                  A1s &&... args) {
  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;
  return __run_and_get_loc(
      is_local,
      +[](T &t, decltype(method_ptr) method_ptr, A0s... args) {
        return (t.*(method_ptr.ptr))(std::move(args)...);
      },
      method_ptr, std::forward<A1s>(args)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT Proclet<T>::run(RetT (T::*md)(A0s...), A1s &&... args) {
  assert_no_pointer_or_lval_ref<RetT, A0s...>();
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(std::move(args)...));

  return __run(md, std::forward<A1s>(args)...);
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT Proclet<T>::__run(RetT (T::*md)(A0s...), A1s &&... args) {
  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;
  return __run(
      +[](T &t, decltype(method_ptr) method_ptr, A0s... args) {
        return (t.*(method_ptr.ptr))(std::move(args)...);
      },
      method_ptr, std::forward<A1s>(args)...);
}

template <typename T>
std::optional<Future<void>> Proclet<T>::update_ref_cnt(int delta) {
  if (Runtime::obj_server) {
    NonBlockingMigrationDisabledGuard callee_guard(to_heap_header(id_));
    if (callee_guard) {
      // Fast path: the heap is actually local, use function call.
      if (likely(ObjServer::update_ref_cnt_locally<T>(id_, delta))) {
        return std::nullopt;
      }
    }
  }

  // Slow path: the heap is actually remote, use RPC.
  return nu::async([&, id = id_, delta]() {
    auto *handler = ObjServer::update_ref_cnt<T>;
    invoke_remote(id, handler, id, delta);
  });
}

template <typename T> void Proclet<T>::reset() {
  if (ref_cnted_) {
    ref_cnted_ = false;
    if (inc_ref_) {
      inc_ref_.get();
    }

    auto dec_ref = update_ref_cnt(-1);
    if (dec_ref) {
      dec_ref->get();
    }
  }
}

template <typename T> std::optional<Future<void>> Proclet<T>::reset_async() {
  if (ref_cnted_) {
    ref_cnted_ = false;
    if (inc_ref_) {
      inc_ref_.get();
    }

    return update_ref_cnt(-1);
  }
}

template <typename T>
template <class Archive>
void Proclet<T>::save(Archive &ar) const {
  const_cast<Proclet<T> *>(this)->ref_cnted_ = false;
  ar(id_);
}

template <typename T>
template <class Archive>
void Proclet<T>::load(Archive &ar) {
  ar(id_);
  ref_cnted_ = true;
}

template <typename T, typename... As> Proclet<T> make_proclet(As &&... args) {
  return Proclet<T>::__create(/* pinned = */ false, 0,
                              std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<Proclet<T>> make_proclet_async(As &&... args) {
  return nu::async([&, ... args = std::forward<As>(args)]() {
    return Proclet<T>::__create(/* pinned = */ false, 0,
                                std::forward<As>(args)...);
  });
}

template <typename T, typename... As>
Proclet<T> make_proclet_at(uint32_t ip_hint, As &&... args) {
  return Proclet<T>::__create(/* pinned = */ false, ip_hint,
                              std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<Proclet<T>> make_proclet_async_at(uint32_t ip_hint, As &&... args) {
  return nu::async([&, ip_hint, ... args = std::forward<As>(args)]() {
    return Proclet<T>::__create(/* pinned = */ false, ip_hint,
                                std::forward<As>(args)...);
  });
}

template <typename T, typename... As>
Proclet<T> make_proclet_pinned(As &&... args) {
  return Proclet<T>::__create(/* pinned = */ true, 0,
                              std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<Proclet<T>> make_proclet_pinned_async(As &&... args) {
  return nu::async([&, ... args = std::forward<As>(args)]() {
    return Proclet<T>::__create(/* pinned = */ true, 0,
                                std::forward<As>(args)...);
  });
}

template <typename T, typename... As>
Proclet<T> make_proclet_pinned_at(uint32_t ip_hint, As &&... args) {
  return Proclet<T>::__create(/* pinned = */ true, ip_hint,
                              std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<Proclet<T>> make_proclet_pinned_async_at(uint32_t ip_hint,
                                                As &&... args) {
  return nu::async([&, ip_hint, ... args = std::forward<As>(args)]() {
    return Proclet<T>::__create(/* pinned = */ true, ip_hint,
                                std::forward<As>(args)...);
  });
}

} // namespace nu

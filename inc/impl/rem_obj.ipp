#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>
#include <array>

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
#include "utils/netaddr.hpp"
#include "utils/promise.hpp"
#include "utils/tcp.hpp"

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

  iovec iovecs[] = {{&states_size, sizeof(states_size)},
                    {const_cast<char *>(states_view.data()), states_size}};
  BUG_ON(conn->WritevFull(iovecs) < 0);

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

template <typename T> RemObj<T>::RemObj(RemObjID id) : id_(id) {
  inc_ref_cnt();
}

template <typename T> RemObj<T>::RemObj() : id_(kNullRemObjID) {}

template <typename T>
RemObj<T>::RemObj(RemObjID id, Future<void> &&construct)
    : id_(id), construct_(std::move(construct)) {}

template <typename T> RemObj<T>::~RemObj() {
  if (construct_) {
    construct_.get();
  }
  if (id_ != kNullRemObjID) {
    dec_ref_cnt();
  }
}

template <typename T> RemObj<T>::RemObj(RemObj<T> &&o) : id_(o.id_) {
  o.id_ = kNullRemObjID;
}

template <typename T> RemObj<T> &RemObj<T>::operator=(RemObj<T> &&o) {
  id_ = o.id_;
  o.id_ = kNullRemObjID;
  return *this;
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create(As &&... args) {
  return general_create(/* pinned = */ false, std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::create_pinned(As &&... args) {
  return general_create(/* pinned = */ true, std::forward<As>(args)...);
}

template <typename T>
template <typename... As>
RemObj<T> RemObj<T>::general_create(bool pinned, As &&... args) {
  auto optional = Runtime::controller_client->allocate_obj();
  BUG_ON(!optional);
  auto [id, range] = *optional;

  Runtime::migration_disable();

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::construct_obj<T, As...>;
  serialize(&oa_sstream->oa, handler, to_heap_base(id), pinned,
            std::forward<As>(args)...);

  auto *construct_promise = Promise<void>::create([id, oa_sstream]() {
    invoke_remote<void>(id, &oa_sstream->ss);
    Runtime::archive_pool->put_oa_sstream(oa_sstream);
    Runtime::migration_enable();
  });
  return RemObj(id, std::move(construct_promise->get_future()));
}

template <typename T> RemObj<T> RemObj<T>::attach(Cap cap) {
  return RemObj<T>(cap.id);
}

template <typename T> RemObj<T>::Cap RemObj<T>::get_cap() {
  if (construct_) {
    construct_.get();
  }

  Cap cap;
  cap.id = id_;
  return cap;
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemObj<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  using closure_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), states...));

  if (construct_) {
    construct_.get();
  }

  Runtime::migration_disable();

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::closure_handler<T, RetT, decltype(fn), S1s...>;
  serialize(&oa_sstream->oa, handler, id_, fn, std::forward<S1s>(states)...);

  auto *promise = Promise<RetT>::create([&, oa_sstream] {
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
  });
  return promise->get_future();
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemObj<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  using closure_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), states...));

  if (construct_) {
    construct_.get();
  }

  Runtime::migration_disable();

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::closure_handler<T, RetT, decltype(fn), S1s...>;
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
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(args...));

  if (construct_) {
    construct_.get();
  }

  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;

  Runtime::migration_disable();
  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler =
      ObjServer::method_handler<T, RetT, decltype(method_ptr), A1s...>;
  serialize(&oa_sstream->oa, handler, id_, method_ptr.raw,
            std::forward<A1s>(args)...);

  auto *promise = Promise<RetT>::create([&, oa_sstream] {
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
  });
  return promise->get_future();
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT RemObj<T>::run(RetT (T::*md)(A0s...), A1s &&... args) {
  using md_args_checker [[maybe_unused]] =
      decltype((std::declval<T>().*(md))(args...));

  if (construct_) {
    construct_.get();
  }

  MethodPtr<decltype(md)> method_ptr;
  method_ptr.ptr = md;

  Runtime::migration_disable();

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler =
      ObjServer::method_handler<T, RetT, decltype(method_ptr), A1s...>;
  serialize(&oa_sstream->oa, handler, id_, method_ptr.raw,
            std::forward<A1s>(args)...);

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
};

template <typename T> void RemObj<T>::inc_ref_cnt() {
  inc_ref_ = std::move(
      update_ref_cnt(1)->template get_future<RuntimeDeleter<Promise<void>>>());
}

template <typename T> void RemObj<T>::dec_ref_cnt() {
  if (inc_ref_) {
    inc_ref_.get();
  }
  auto *dec_promise = update_ref_cnt(-1);
  Runtime::rcu_lock.reader_lock();
  rt::Thread([=]() {
    dec_promise->template get_future<RuntimeDeleter<Promise<void>>>().get();
    Runtime::rcu_lock.reader_unlock();
  }).Detach();
}

template <typename T> Promise<void> *RemObj<T>::update_ref_cnt(int delta) {
  Runtime::migration_disable();

  auto *oa_sstream = Runtime::archive_pool->get_oa_sstream();
  auto *handler = ObjServer::update_ref_cnt<T>;
  serialize(&oa_sstream->oa, handler, id_, delta);

  return Promise<void>::create<RuntimeAllocator<Promise<void>>>(
      [&, id = id_, oa_sstream] {
        invoke_remote<void>(id, &oa_sstream->ss);
        Runtime::archive_pool->put_oa_sstream(oa_sstream);
        Runtime::migration_enable();
      });
}
} // namespace nu

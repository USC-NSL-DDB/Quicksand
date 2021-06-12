#pragma once

#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <optional>

extern "C" {
#include <runtime/tcp.h>
}

#include "defs.hpp"
#include "runtime_deleter.hpp"
#include "utils/future.hpp"
#include "utils/promise.hpp"

namespace nu {

template <typename T> class RemObj {
public:
  struct Cap {
    RemObjID id;

    template <class Archive> void serialize(Archive &ar) { ar(id); }
  };

  RemObj(const Cap &cap);
  RemObj(const RemObj &) = delete;
  RemObj &operator=(const RemObj &) = delete;
  RemObj(RemObj &&);
  RemObj &operator=(RemObj &&);
  RemObj();
  ~RemObj();
  template <typename... As> static RemObj create(As &&... args);
  template <typename... As>
  static RemObj create_at(netaddr addr, As &&... args);
  template <typename... As> static RemObj create_pinned(As &&... args);
  template <typename... As>
  static RemObj create_pinned_at(netaddr addr, As &&... args);
  Cap get_cap();
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...), A1s &&... args);
  bool is_local() const;

private:
  RemObjID id_;
  Future<void> construct_;
  Future<void, RuntimeDeleter<nu::Promise<void>>> inc_ref_;
  template <typename K, typename V, typename Hash, typename KeyEqual>
  friend class DistributedHashTable;
  template <typename U> friend class RemPtr;

  RemObj(RemObjID id);
  RemObj(RemObjID id, Future<void> &&construct);
  Promise<void> *update_ref_cnt(int delta);
  template <typename RetT>
  static RetT invoke_remote(RemObjID id, auto *states_ss);
  template <typename... As>
  static RemObj general_create(bool pinned, std::optional<netaddr> hint,
                               As &&... args);
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> __run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT __run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> __run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT __run(RetT (T::*md)(A0s...), A1s &&... args);
  bool __is_local() const;
};

template <typename T> union MethodPtr {
  T ptr;
  uint8_t raw[sizeof(T)];

  template <class Archive> void serialize(Archive &ar) { ar(raw); }
};

} // namespace nu

#include "impl/rem_obj.ipp"

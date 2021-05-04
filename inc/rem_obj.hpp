#pragma once

extern "C" {
#include <runtime/tcp.h>
}

#include <cereal/archives/binary.hpp>
#include <cstdint>

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

  RemObj(const RemObj &) = delete;
  RemObj &operator=(const RemObj &) = delete;
  RemObj(RemObj &&);
  RemObj &operator=(RemObj &&);
  RemObj();
  ~RemObj();
  template <typename... As> static RemObj create(As &&... args);
  template <typename... As> static RemObj create_pinned(As &&... args);
  static RemObj attach(Cap cap);
  Cap get_cap();
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...), A1s &&... args);

private:
  RemObjID id_;
  Future<void> construct_;
  Future<void, RuntimeDeleter<nu::Promise<void>>> inc_ref_;

  RemObj(RemObjID id);
  RemObj(RemObjID id, Future<void> &&construct);
  void inc_ref_cnt();
  void dec_ref_cnt();
  Promise<void> *update_ref_cnt(int delta);
  template <typename RetT>
  static RetT invoke_remote(RemObjID id, auto *states_ss);
  template <typename... As>
  static RemObj general_create(bool pinned, As &&... args);
};

template <typename T> union MethodPtr {
  T ptr;
  uint8_t raw[sizeof(T)];
};

} // namespace nu

#include "impl/rem_obj.ipp"

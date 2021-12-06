#pragma once

#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <optional>

#include "nu/commons.hpp"
#include "nu/runtime_deleter.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/promise.hpp"

namespace nu {

template <typename T> class RemObj {
public:
  struct Cap {
    RemObjID id;

    bool operator==(const Cap &o) const { return id == o.id; }
    template <class Archive> void serialize(Archive &ar) { ar(id); }
  };

  RemObj(const Cap &cap, bool ref_cnted = true);
  RemObj(const RemObj &) = delete;
  RemObj &operator=(const RemObj &) = delete;
  RemObj(RemObj &&);
  RemObj &operator=(RemObj &&);
  RemObj();
  ~RemObj();
  template <typename... As> static RemObj create(As &&... args);
  template <typename... As> static Future<RemObj> create_async(As &&... args);
  template <typename... As>
  static RemObj create_at(netaddr addr, As &&... args);
  template <typename... As>
  static Future<RemObj> create_at_async(netaddr addr, As &&... args);
  template <typename... As> static RemObj create_pinned(As &&... args);
  template <typename... As>
  static Future<RemObj> create_pinned_async(As &&... args);
  template <typename... As>
  static RemObj create_pinned_at(netaddr addr, As &&... args);
  template <typename... As>
  static Future<RemObj> create_pinned_at_async(netaddr addr, As &&... args);
  Cap get_cap() const;
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...), A1s &&... args);
  void reset();
  Future<void> reset_async();
  void reset_bg();

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void load(Archive &ar);

private:
  RemObjID id_;
  Future<void> inc_ref_;
  bool ref_cnted_;

  template <typename U> friend class RemPtr;
  template <typename K, typename V, typename Hash, typename KeyEqual,
            uint64_t NumBuckets>
  friend class DistributedHashTable;
  friend class DistributedMemPool;

  RemObj(RemObjID id, bool ref_cnted);
  Promise<void> *update_ref_cnt(int delta);
  template <typename... S1s>
  static void invoke_remote(RemObjID id, S1s &&... states);
  template <typename RetT, typename... S1s>
  static RetT invoke_remote_with_ret(RemObjID id, S1s &&... states);
  template <typename... As>
  static RemObj general_create(bool pinned, std::optional<netaddr> hint,
                               As &&... args);
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> __run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT __run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT __run_and_get_loc(bool *is_local, RetT (*fn)(T &, S0s...),
                         S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> __run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT __run(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT __run_and_get_loc(bool *is_local, RetT (T::*md)(A0s...), A1s &&... args);
};

template <typename T> union MethodPtr {
  T ptr;
  uint8_t raw[sizeof(T)];

  template <class Archive> void serialize(Archive &ar) { ar(raw); }
};

} // namespace nu

#include "nu/impl/rem_obj.ipp"

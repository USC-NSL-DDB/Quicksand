#pragma once

#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <optional>

#include "nu/commons.hpp"
#include "nu/runtime_deleter.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/promise.hpp"

namespace nu {

template <typename T> class Proclet {
public:
  struct Cap {
    ProcletID id;

    bool operator==(const Cap &o) const { return id == o.id; }
    template <class Archive> void serialize(Archive &ar) { ar(id); }
  };

  Proclet(const Cap &cap, bool ref_cnted = true);
  Proclet(const Proclet &) = delete;
  Proclet &operator=(const Proclet &) = delete;
  Proclet(Proclet &&);
  Proclet &operator=(Proclet &&);
  Proclet();
  ~Proclet();
  template <typename... As> static Proclet create(As &&... args);
  template <typename... As> static Future<Proclet> create_async(As &&... args);
  template <typename... As>
  static Proclet create_at(uint32_t ip, As &&... args);
  template <typename... As>
  static Future<Proclet> create_at_async(uint32_t ip, As &&... args);
  template <typename... As> static Proclet create_pinned(As &&... args);
  template <typename... As>
  static Future<Proclet> create_pinned_async(As &&... args);
  template <typename... As>
  static Proclet create_pinned_at(uint32_t ip, As &&... args);
  template <typename... As>
  static Future<Proclet> create_pinned_at_async(uint32_t ip, As &&... args);
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

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void load(Archive &ar);

private:
  ProcletID id_;
  Future<void> inc_ref_;
  bool ref_cnted_;

  template <typename U> friend class RemPtr;
  template <typename K, typename V, typename Hash, typename KeyEqual,
            uint64_t NumBuckets>
  friend class DistributedHashTable;
  friend class DistributedMemPool;

  Proclet(ProcletID id, bool ref_cnted);
  Promise<void> *update_ref_cnt(int delta);
  template <typename... S1s>
  static void invoke_remote(ProcletID id, S1s &&... states);
  template <typename RetT, typename... S1s>
  static RetT invoke_remote_with_ret(ProcletID id, S1s &&... states);
  template <typename... As>
  static Proclet general_create(bool pinned, uint32_t ip_hint, As &&... args);
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

#include "nu/impl/proclet.ipp"

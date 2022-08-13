#pragma once

#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <optional>

#include "nu/commons.hpp"
#include "nu/runtime_deleter.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename T>
class WeakProclet;

template <typename T>
class Proclet {
 public:
  Proclet(const Proclet &);
  Proclet &operator=(const Proclet &);
  Proclet(Proclet &&) noexcept;
  Proclet &operator=(Proclet &&) noexcept;
  Proclet();
  ~Proclet();
  ProcletID get_id() const;
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT run(RetT (T::*md)(A0s...), A1s &&... args);
  void reset();
  std::optional<Future<void>> reset_async();
  WeakProclet<T> get_weak();

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void save_move(Archive &ar);
  template <class Archive>
  void load(Archive &ar);

 private:
  ProcletID id_;

  template <typename U>
  friend class WeakProclet;
  template <typename U>
  friend class RemPtr;
  template <typename K, typename V, typename Hash, typename KeyEqual,
            uint64_t NumBuckets>
  friend class DistributedHashTable;
  friend class DistributedMemPool;

  std::optional<Future<void>> update_ref_cnt(ProcletID id, int delta);
  template <typename... S1s>
  static void invoke_remote(ProcletID id, S1s &&... states);
  template <typename RetT, typename... S1s>
  static RetT invoke_remote_with_ret(ProcletID id, S1s &&... states);
  template <typename... As>
  static Proclet __create(uint64_t capacity, bool pinned, uint32_t ip_hint,
                          As &&... args);
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> __run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT __run(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... A0s, typename... A1s>
  Future<RetT> __run_async(RetT (T::*md)(A0s...), A1s &&... args);
  template <typename RetT, typename... A0s, typename... A1s>
  RetT __run(RetT (T::*md)(A0s...), A1s &&... args);

  template <typename U, typename... As>
  friend Proclet<U> make_proclet(As &&... args);
  template <typename U, typename... As>
  friend Future<Proclet<U>> make_proclet_async(As &&... args);
  template <typename U, typename... As>
  friend Proclet<U> make_proclet_at(uint32_t ip, As &&... args);
  template <typename U, typename... As>
  friend Future<Proclet<U>> make_proclet_async_at(uint32_t ip, As &&... args);
  template <typename U, typename... As>
  friend Proclet<U> make_proclet_pinned(As &&... args);
  template <typename U, typename... As>
  friend Future<Proclet<U>> make_proclet_pinned_async(As &&... args);
  template <typename U, typename... As>
  friend Proclet<U> make_proclet_pinned_at(uint32_t ip, As &&... args);
  template <typename U, typename... As>
  friend Future<Proclet<U>> make_proclet_pinned_async_at(uint32_t ip,
                                                         As &&... args);
  template <typename U, typename... As>
  friend Proclet<U> make_proclet_with_capacity(uint64_t capacity,
                                               As &&... args);
  template <typename U, typename... As>
  friend Future<Proclet<U>> make_proclet_async_with_capacity(uint64_t capacity,
                                                             As &&... args);
};

template <typename T>
class WeakProclet : public Proclet<T> {
 public:
  WeakProclet();
  ~WeakProclet();
  WeakProclet(const Proclet<T> &proclet);
  WeakProclet(const WeakProclet<T> &proclet);
  WeakProclet &operator=(const WeakProclet<T> &proclet);

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void save_move(Archive &ar);
  template <class Archive>
  void load(Archive &ar);

 private:
  template <typename U>
  friend class RemPtr;

  WeakProclet(ProcletID id);
};

template <typename T, typename... As>
Proclet<T> make_proclet(As &&... args);
template <typename T, typename... As>
Future<Proclet<T>> make_proclet_async(As &&... args);
template <typename T, typename... As>
Proclet<T> make_proclet_at(uint32_t ip, As &&... args);
template <typename T, typename... As>
Future<Proclet<T>> make_proclet_async_at(uint32_t ip, As &&... args);
template <typename T, typename... As>
Proclet<T> make_proclet_pinned(As &&... args);
template <typename T, typename... As>
Future<Proclet<T>> make_proclet_pinned_async(As &&... args);
template <typename T, typename... As>
Proclet<T> make_proclet_pinned_at(uint32_t ip, As &&... args);
template <typename T, typename... As>
Future<Proclet<T>> make_proclet_pinned_async_at(uint32_t ip, As &&... args);
template <typename T, typename... As>
Proclet<T> make_proclet_with_capacity(uint64_t capacity, As &&... args);
template <typename T, typename... As>
Future<Proclet<T>> make_proclet_async_with_capacity(uint64_t capacity,
                                                    As &&... args);

}  // namespace nu

#include "nu/impl/proclet.ipp"

#pragma once

#include <memory>

#include "nu/proclet.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename T> class RemPtr {
public:
  RemPtr() noexcept;
  RemPtr(const RemPtr &) noexcept;
  RemPtr &operator=(const RemPtr &) noexcept;
  RemPtr(RemPtr &&) noexcept;
  RemPtr &operator=(RemPtr &&) noexcept;
  operator bool() const;
  T operator*();
  T *get();
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&... states);

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void load(Archive &ar);

protected:
  ProcletID proclet_id_;
  T *raw_ptr_ = nullptr;

  RemPtr(ProcletID id, T *raw_ptr);
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> __run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT __run(RetT (*fn)(T &, S0s...), S1s &&... states);

private:
  friend class DistributedMemPool;
};

} // namespace nu

#include "nu/impl/rem_ptr.ipp"


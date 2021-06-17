#pragma once

#include <memory>

#include "rem_obj.hpp"
#include "utils/future.hpp"

namespace nu {

template <typename T> class RemPtr {
public:
  RemPtr();
  RemPtr(const RemPtr &);
  RemPtr &operator=(const RemPtr &);
  RemPtr(RemPtr &&);
  RemPtr &operator=(RemPtr &&);
  operator bool() const;
  T operator*();
  bool is_local() const;
  T *get();
  T *get_checked();
  template <typename RetT, typename... S0s, typename... S1s>
  Future<RetT> run_async(RetT (*fn)(T &, S0s...), S1s &&... states);
  template <typename RetT, typename... S0s, typename... S1s>
  RetT run(RetT (*fn)(T &, S0s...), S1s &&... states);

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void load(Archive &ar);

private:
  RemObjID rem_obj_id_;
  T *raw_ptr_;
  template <typename U> friend RemPtr<U> to_rem_ptr(U *raw_ptr);
  friend class DistributedMemPool;

  // Can only be invoked through to_rem_ptr locally.
  RemPtr(RemObjID id, T *raw_ptr);
};

template <typename T> RemPtr<T> to_rem_ptr(T *raw_ptr);

} // namespace nu

#include "impl/rem_ptr.ipp"

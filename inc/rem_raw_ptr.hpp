#pragma once

#include <memory>

#include "rem_obj.hpp"
#include "utils/future.hpp"

namespace nu {

template <typename T> class RemRawPtr {
public:
  RemRawPtr();
  RemRawPtr(const RemRawPtr &);
  RemRawPtr &operator=(const RemRawPtr &);
  RemRawPtr(RemRawPtr &&);
  RemRawPtr &operator=(RemRawPtr &&);
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
  template <typename U> friend RemRawPtr<U> to_rem_raw_ptr(U *raw_ptr);
  friend class DistributedMemPool;

  // Can only be invoked through to_rem_raw_ptr locally.
  RemRawPtr(RemObjID id, T *raw_ptr);
};

template <typename T> RemRawPtr<T> to_rem_raw_ptr(T *raw_ptr);

} // namespace nu

#include "impl/rem_raw_ptr.ipp"

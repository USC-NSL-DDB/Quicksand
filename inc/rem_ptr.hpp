#pragma once

#include <memory>

#include "rem_obj.hpp"
#include "utils/future.hpp"

namespace nu {

template <typename T> class RemPtr {
public:
  RemPtr();
  RemPtr(RemObjID id, T *raw_ptr);
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

  template <class Archive> void serialize(Archive &ar) { ar(id_, raw_ptr_); }

private:
  struct ErasedType {};

  RemObjID id_;
  T *raw_ptr_;
  RemObj<ErasedType> rem_obj_;
};

template <typename T> RemPtr<T> to_rem_ptr(T *raw_ptr);

} // namespace nu

#include "impl/rem_ptr.ipp"

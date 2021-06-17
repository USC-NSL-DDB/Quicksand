#include "defs.hpp"
#include "runtime.hpp"

namespace nu {

template <typename T>
template <class Archive>
void RemRawPtr<T>::save(Archive &ar) const {
  ar(raw_ptr_, rem_obj_id_);
}

template <typename T>
template <class Archive>
void RemRawPtr<T>::load(Archive &ar) {
  RemObjID id;
  ar(raw_ptr_, id);
  rem_obj_id_ = id;
}

template <typename T> RemRawPtr<T>::RemRawPtr() : raw_ptr_(nullptr) {}

template <typename T>
RemRawPtr<T>::RemRawPtr(const RemRawPtr<T> &o)
    : rem_obj_id_(o.rem_obj_id_), raw_ptr_(o.raw_ptr_) {}

template <typename T>
RemRawPtr<T> &RemRawPtr<T>::operator=(const RemRawPtr<T> &o) {
  rem_obj_id_ = o.rem_obj_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemRawPtr<T>::RemRawPtr(RemRawPtr<T> &&o)
    : rem_obj_id_(o.rem_obj_id_), raw_ptr_(o.raw_ptr_) {}

template <typename T> RemRawPtr<T> &RemRawPtr<T>::operator=(RemRawPtr<T> &&o) {
  rem_obj_id_ = o.rem_obj_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemRawPtr<T>::RemRawPtr(RemObjID id, T *raw_ptr)
    : rem_obj_id_(id), raw_ptr_(raw_ptr) {}

template <typename T> RemRawPtr<T>::operator bool() const { return raw_ptr_; }

template <typename T> bool RemRawPtr<T>::is_local() const {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.is_local();
}

template <typename T> T *RemRawPtr<T>::get() { return raw_ptr_; }

template <typename T> T *RemRawPtr<T>::get_checked() {
  BUG_ON(!is_local());
  return raw_ptr_;
}

template <typename T> T RemRawPtr<T>::operator*() {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.__run(
      +[](ErasedType &, T *raw_ptr) { return *raw_ptr; }, raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemRawPtr<T>::run_async(RetT (*fn)(T &, S0s...),
                                     S1s &&... states) {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.__run_async(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &... states) {
        return fn(*raw_ptr, states...);
      },
      raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemRawPtr<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.__run(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &... states) {
        return fn(*raw_ptr, states...);
      },
      raw_ptr_);
}

template <typename T> RemRawPtr<T> to_rem_raw_ptr(T *raw_ptr) {
  auto *heap_base = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_base);
  auto id = to_obj_id(heap_base);
  return RemRawPtr<T>(id, raw_ptr);
}
} // namespace nu

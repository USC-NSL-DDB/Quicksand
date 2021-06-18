#include "defs.hpp"
#include "runtime.hpp"

namespace nu {

template <typename T>
template <class Archive>
void RemPtr<T>::save(Archive &ar) const {
  ar(raw_ptr_, rem_obj_id_);
}

template <typename T>
template <class Archive>
void RemPtr<T>::load(Archive &ar) {
  RemObjID id;
  ar(raw_ptr_, id);
  rem_obj_id_ = id;
}

template <typename T> RemPtr<T>::RemPtr() : raw_ptr_(nullptr) {}

template <typename T>
RemPtr<T>::RemPtr(const RemPtr<T> &o)
    : rem_obj_id_(o.rem_obj_id_), raw_ptr_(o.raw_ptr_) {}

template <typename T> RemPtr<T> &RemPtr<T>::operator=(const RemPtr<T> &o) {
  rem_obj_id_ = o.rem_obj_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(RemPtr<T> &&o)
    : rem_obj_id_(o.rem_obj_id_), raw_ptr_(o.raw_ptr_) {}

template <typename T> RemPtr<T> &RemPtr<T>::operator=(RemPtr<T> &&o) {
  rem_obj_id_ = o.rem_obj_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(RemObjID id, T *raw_ptr)
    : rem_obj_id_(id), raw_ptr_(raw_ptr) {}

template <typename T> RemPtr<T>::operator bool() const { return raw_ptr_; }

template <typename T> bool RemPtr<T>::is_local() const {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.is_local();
}

template <typename T> T *RemPtr<T>::get() { return raw_ptr_; }

template <typename T> T *RemPtr<T>::get_checked() {
  BUG_ON(!is_local());
  return raw_ptr_;
}

template <typename T> T RemPtr<T>::operator*() {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.__run(
      +[](ErasedType &, T *raw_ptr) { return *raw_ptr; }, raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemPtr<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.__run_async(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &&... states) {
        return fn(*raw_ptr, std::forward<S1s>(states)...);
      },
      raw_ptr_, fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemPtr<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  RemObj<ErasedType> rem_obj(rem_obj_id_, false);
  return rem_obj.__run(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &&... states) {
        return fn(*raw_ptr, std::forward<S1s>(states)...);
      },
      raw_ptr_, fn, std::forward<S1s>(states)...);
}

} // namespace nu

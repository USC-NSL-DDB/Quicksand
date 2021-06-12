#include "defs.hpp"
#include "runtime.hpp"

namespace nu {

template <typename T>
template <class Archive>
void RemPtr<T>::save(Archive &ar) const {
  ar(raw_ptr_, rem_obj_.id_);
}

template <typename T>
template <class Archive>
void RemPtr<T>::load(Archive &ar) {
  RemObjID id;
  ar(raw_ptr_, id);
  rem_obj_ = std::move(RemObj<ErasedType>(id));
}

template <typename T>
RemPtr<T>::RemPtr() : raw_ptr_(nullptr) {}

template <typename T>
RemPtr<T>::RemPtr(const RemPtr<T> &o)
    : raw_ptr_(o.raw_ptr_), rem_obj_(o.rem_obj_.id_) {}

template <typename T> RemPtr<T> &RemPtr<T>::operator=(const RemPtr<T> &o) {
  raw_ptr_ = o.raw_ptr_;
  rem_obj_ = std::move(RemObj<T>(o.rem_obj_.id_));
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(RemPtr<T> &&o)
    : raw_ptr_(o.raw_ptr_), rem_obj_(std::move(o.rem_obj_)) {}

template <typename T> RemPtr<T> &RemPtr<T>::operator=(RemPtr<T> &&o) {
  raw_ptr_ = o.raw_ptr_;
  rem_obj_ = std::move(o.rem_obj_);
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(RemObjID id, T *raw_ptr) : raw_ptr_(raw_ptr) {
  // Now the heap is actually local. Therefore there is no need to inc its
  // ref count, and we intentionally don't invoke rem_obj_'s constructor.
  rem_obj_.id_ = id;
}

template <typename T> RemPtr<T>::operator bool() const { return raw_ptr_; }

template <typename T> bool RemPtr<T>::is_local() const {
  return rem_obj_.is_local();
}

template <typename T> T *RemPtr<T>::get() { return raw_ptr_; }

template <typename T> T *RemPtr<T>::get_checked() {
  BUG_ON(!is_local());
  return raw_ptr_;
}

template <typename T> T RemPtr<T>::operator*() {
  return rem_obj_.__run(
      +[](ErasedType &, T *raw_ptr) { return *raw_ptr; }, raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemPtr<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  return rem_obj_.__run_async(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &... states) {
        return fn(*raw_ptr, states...);
      },
      raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemPtr<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  return rem_obj_.__run(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &... states) {
        return fn(*raw_ptr, states...);
      },
      raw_ptr_);
}

template <typename T> RemPtr<T> to_rem_ptr(T *raw_ptr) {
  auto *heap_base = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_base);
  auto id = to_obj_id(heap_base);
  return RemPtr<T>(id, raw_ptr);
}
} // namespace nu

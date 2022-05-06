#include "nu/commons.hpp"
#include "nu/runtime.hpp"

namespace nu {

template <typename T>
template <class Archive>
void RemPtr<T>::save(Archive &ar) const {
  ar(raw_ptr_, proclet_id_);
}

template <typename T>
template <class Archive>
void RemPtr<T>::load(Archive &ar) {
  ProcletID id;
  ar(raw_ptr_, id);
  proclet_id_ = id;
}

template <typename T> RemPtr<T>::RemPtr() noexcept {}

template <typename T>
RemPtr<T>::RemPtr(const RemPtr<T> &o) noexcept
    : proclet_id_(o.proclet_id_), raw_ptr_(o.raw_ptr_) {}

template <typename T>
RemPtr<T> &RemPtr<T>::operator=(const RemPtr<T> &o) noexcept {
  proclet_id_ = o.proclet_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(RemPtr<T> &&o) noexcept
    : proclet_id_(o.proclet_id_), raw_ptr_(o.raw_ptr_) {}

template <typename T> RemPtr<T> &RemPtr<T>::operator=(RemPtr<T> &&o) noexcept {
  proclet_id_ = o.proclet_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(ProcletID id, T *raw_ptr)
    : proclet_id_(id), raw_ptr_(raw_ptr) {}

template <typename T> RemPtr<T>::operator bool() const { return raw_ptr_; }

template <typename T> T *RemPtr<T>::get() { return raw_ptr_; }

template <typename T> T RemPtr<T>::operator*() {
  Proclet<ErasedType> proclet(proclet_id_, false);
  return proclet.__run(
      +[](ErasedType &, T *raw_ptr) { return *raw_ptr; }, raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemPtr<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  assert_no_pointer_or_lval_ref<RetT, S0s...>();
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::forward<S1s>(states)...));

  return __run_async(fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemPtr<T>::__run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  Proclet<ErasedType> proclet(proclet_id_, false);
  return proclet.__run_async(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &&... states) {
        return fn(*raw_ptr, std::forward<S1s>(states)...);
      },
      raw_ptr_, fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemPtr<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  assert_no_pointer_or_lval_ref<RetT, S0s...>();
  using fn_states_checker [[maybe_unused]] =
      decltype(fn(std::declval<T &>(), std::forward<S1s>(states)...));

  return __run(fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemPtr<T>::__run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  Proclet<ErasedType> proclet(proclet_id_, false);
  return proclet.__run(
      +[](ErasedType &, T *raw_ptr, RetT (*fn)(T &, S0s...), S1s &&... states) {
        return fn(*raw_ptr, std::forward<S1s>(states)...);
      },
      raw_ptr_, fn, std::forward<S1s>(states)...);
}

} // namespace nu

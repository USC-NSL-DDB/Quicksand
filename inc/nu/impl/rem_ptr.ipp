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

template <typename T>
RemPtr<T>::RemPtr() noexcept {}

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

template <typename T>
RemPtr<T> &RemPtr<T>::operator=(RemPtr<T> &&o) noexcept {
  proclet_id_ = o.proclet_id_;
  raw_ptr_ = o.raw_ptr_;
  return *this;
}

template <typename T>
RemPtr<T>::RemPtr(ProcletID id, T *raw_ptr)
    : proclet_id_(id), raw_ptr_(raw_ptr) {}

template <typename T>
RemPtr<T>::operator bool() const {
  return raw_ptr_;
}

template <typename T>
T *RemPtr<T>::get() {
  return raw_ptr_;
}

template <typename T>
T RemPtr<T>::operator*() {
  Proclet<ErasedType> proclet(proclet_id_, false);
  return proclet.__run(
      +[](ErasedType &, T *raw_ptr) { return *raw_ptr; }, raw_ptr_);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
Future<RetT> RemPtr<T>::run_async(RetT (*fn)(T &, S0s...), S1s &&... states) {
  Proclet<ErasedType> proclet(proclet_id_, false);
  auto raw_ptr_addr = reinterpret_cast<uintptr_t>(raw_ptr_);
  return proclet.__run_async(
      +[](ErasedType &, uintptr_t raw_ptr_addr, RetT (*fn)(T &, S0s...),
          S1s &&... states) {
        auto *raw_ptr = reinterpret_cast<T *>(raw_ptr_addr);
        return fn(*raw_ptr, std::forward<S1s>(states)...);
      },
      raw_ptr_addr, fn, std::forward<S1s>(states)...);
}

template <typename T>
template <typename RetT, typename... S0s, typename... S1s>
RetT RemPtr<T>::run(RetT (*fn)(T &, S0s...), S1s &&... states) {
  Proclet<ErasedType> proclet(proclet_id_, false);
  auto raw_ptr_addr = reinterpret_cast<uintptr_t>(raw_ptr_);
  return proclet.__run(
      +[](ErasedType &, uintptr_t raw_ptr_addr, RetT (*fn)(T &, S0s...),
          S0s... states) {
        auto *raw_ptr = reinterpret_cast<T *>(raw_ptr_addr);
        return fn(*raw_ptr, std::move(states)...);
      },
      raw_ptr_addr, fn, std::forward<S1s>(states)...);
}

}  // namespace nu

#include "nu/dis_mem_pool.hpp"

namespace nu {

template <typename T>
template <class Archive>
void RemSharedPtr<T>::save(Archive &ar) const {
  const_cast<RemSharedPtr<T> *>(this)->save(ar);
}

template <typename T>
template <class Archive>
void RemSharedPtr<T>::save(Archive &ar) {
  RemPtr<T>::save(ar);
  ar(shared_ptr_);
  RemPtr<T>::raw_ptr_ = nullptr;
}

template <typename T>
template <class Archive>
void RemSharedPtr<T>::load(Archive &ar) {
  RemPtr<T>::load(ar);
  ar(shared_ptr_);
}

template <typename T>
consteval auto get_reset_fn() {
  return +[](T &t, std::shared_ptr<T> *shared_ptr) { delete shared_ptr; };
}

template <typename T>
consteval auto get_copy_shared_ptr_fn() {
  return +[](T &t, std::shared_ptr<T> *shared_ptr) {
    return new std::shared_ptr<T>(*shared_ptr);
  };
}

template <typename T>
RemSharedPtr<T>::RemSharedPtr() noexcept {}

template <typename T>
RemSharedPtr<T>::RemSharedPtr(std::shared_ptr<T> &&shared_ptr) noexcept
    : RemPtr<T>(Runtime::get_current_proclet_id(), shared_ptr.get()),
      shared_ptr_(new std::shared_ptr<T>(std::move(shared_ptr))) {}

template <typename T>
RemSharedPtr<T>::RemSharedPtr(std::shared_ptr<T> *shared_ptr)
    : RemPtr<T>(Runtime::get_current_proclet_id(),
                shared_ptr ? shared_ptr->get() : nullptr),
      shared_ptr_(shared_ptr) {}

template <typename T>
RemSharedPtr<T>::~RemSharedPtr() noexcept {
  reset();
}

template <typename T>
RemSharedPtr<T>::RemSharedPtr(const RemSharedPtr<T> &o) noexcept
    : RemPtr<T>(o) {
  shared_ptr_ = RemPtr<T>::run(get_copy_shared_ptr_fn<T>(), o.shared_ptr_);
}

template <typename T>
RemSharedPtr<T> &RemSharedPtr<T>::operator=(const RemSharedPtr<T> &o) noexcept {
  reset();
  RemPtr<T>::operator=(o);
  shared_ptr_ = RemPtr<T>::run(get_copy_shared_ptr_fn<T>(), o.shared_ptr_);
  return *this;
}

template <typename T>
RemSharedPtr<T>::RemSharedPtr(RemSharedPtr<T> &&o) noexcept
    : RemPtr<T>(std::move(o)), shared_ptr_(o.shared_ptr_) {
  o.raw_ptr_ = nullptr;
}

template <typename T>
RemSharedPtr<T> &RemSharedPtr<T>::operator=(RemSharedPtr<T> &&o) noexcept {
  reset();
  RemPtr<T>::operator=(std::move(o));
  shared_ptr_ = o.shared_ptr_;
  o.raw_ptr_ = nullptr;
  return *this;
}

template <typename T>
void RemSharedPtr<T>::reset() {
  if (RemPtr<T>::get()) {
    RemPtr<T>::run(get_reset_fn<T>(), shared_ptr_);
    RemPtr<T>::raw_ptr_ = nullptr;
  }
}

template <typename T>
Future<void> RemSharedPtr<T>::reset_async() {
  if (RemPtr<T>::get()) {
    auto future = RemPtr<T>::run_async(get_reset_fn<T>(), shared_ptr_);
    RemPtr<T>::raw_ptr_ = nullptr;
    return future;
  } else {
    RemPtr<T>::raw_ptr_ = nullptr;
    return run_async(+[](T &t) {});
  }
}

template <typename T, typename... Args>
RemSharedPtr<T> make_rem_shared(Args &&... args) {
  auto *raw_ptr = new (std::nothrow) T(std::forward<Args>(args)...);
  if (unlikely(!raw_ptr)) {
    return RemSharedPtr<T>();
  }
  auto *shared_ptr = new (std::nothrow) std::shared_ptr<T>(raw_ptr);
  if (unlikely(!shared_ptr)) {
    return RemSharedPtr<T>();
  }
  return RemSharedPtr<T>(shared_ptr);
}

}  // namespace nu

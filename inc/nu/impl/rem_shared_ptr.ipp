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

template <typename T> consteval auto get_reset_fn() {
  return +[](T &t, std::shared_ptr<T> *shared_ptr) { delete shared_ptr; };
}

template <typename T> consteval auto get_copy_shared_ptr_fn() {
  return +[](T &t, std::shared_ptr<T> *shared_ptr) {
    return new std::shared_ptr<T>(*shared_ptr);
  };
}

template <typename T> RemSharedPtr<T>::RemSharedPtr() noexcept {}

template <typename T>
RemSharedPtr<T>::RemSharedPtr(std::shared_ptr<T> &&shared_ptr) noexcept
    : RemPtr<T>(Runtime::get_current_obj_id(), shared_ptr.get()),
      shared_ptr_(new std::shared_ptr<T>(std::move(shared_ptr))) {}

template <typename T>
RemSharedPtr<T>::RemSharedPtr(std::shared_ptr<T> *shared_ptr)
    : RemPtr<T>(Runtime::get_current_obj_id(),
                shared_ptr ? shared_ptr->get() : nullptr),
      shared_ptr_(shared_ptr) {}

template <typename T> RemSharedPtr<T>::~RemSharedPtr() noexcept { reset(); }

template <typename T>
RemSharedPtr<T>::RemSharedPtr(const RemSharedPtr<T> &o) noexcept
    : RemPtr<T>(o) {
  shared_ptr_ = RemPtr<T>::__run(get_copy_shared_ptr_fn<T>(), o.shared_ptr_);
}

template <typename T>
RemSharedPtr<T> &RemSharedPtr<T>::operator=(const RemSharedPtr<T> &o) noexcept {
  reset();
  RemPtr<T>::operator=(o);
  shared_ptr_ = RemPtr<T>::__run(get_copy_shared_ptr_fn<T>(), o.shared_ptr_);
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

template <typename T> void RemSharedPtr<T>::reset() {
  if (RemPtr<T>::get()) {
    RemPtr<T>::__run(get_reset_fn<T>(), shared_ptr_);
    RemPtr<T>::raw_ptr_ = nullptr;
  }
}

template <typename T> Future<void> RemSharedPtr<T>::reset_async() {
  if (RemPtr<T>::get()) {
    auto future = RemPtr<T>::__run_async(get_reset_fn<T>(), shared_ptr_);
    RemPtr<T>::raw_ptr_ = nullptr;
    return future;
  } else {
    RemPtr<T>::raw_ptr_ = nullptr;
    return run_async(+[](T &t) {});
  }
}

template <typename T> void RemSharedPtr<T>::reset_bg() {
  if (RemPtr<T>::get()) {
    // Should allocate from the runtime slab, since the root object might be
    // destructed earlier than this background thread.
    RuntimeSlabGuard guard;
    Runtime::rcu_lock.reader_lock();
    rt::Thread([shared_ptr = shared_ptr_,
                rem_ptr = *static_cast<RemPtr<T> *>(this)]() {
      rem_ptr.__run(get_reset_fn<T>(), shared_ptr);
      Runtime::rcu_lock.reader_unlock();
    }).Detach();
    RemPtr<T>::raw_ptr_ = nullptr;
  }
}

template <typename T, typename... Args>
RemSharedPtr<T> make_rem_shared(Args &&... args) {
  try {
    auto *shared_ptr = new std::shared_ptr<T>(
        std::make_shared<T>(std::forward<Args>(args)...));
    return RemSharedPtr<T>(shared_ptr);
  } catch (std::bad_alloc &) {
    return RemSharedPtr<T>();
  }
}

} // namespace nu

#include "nu/dis_mem_pool.hpp"

namespace nu {

template <typename T>
template <class Archive>
void RemUniquePtr<T>::save(Archive &ar) const {
  const_cast<RemUniquePtr<T> *>(this)->save(ar);
}

template <typename T>
template <class Archive>
void RemUniquePtr<T>::save(Archive &ar) {
  RemPtr<T>::save(ar);
  release();
}

template <typename T> consteval auto get_free_fn() {
  return +[](T &t) { delete &t; };
}

template <typename T> RemUniquePtr<T>::RemUniquePtr() noexcept {}

template <typename T>
RemUniquePtr<T>::RemUniquePtr(T *raw_ptr)
    : RemPtr<T>(Runtime::get_current_obj_id(), raw_ptr) {}

template <typename T>
RemUniquePtr<T>::RemUniquePtr(std::unique_ptr<T> &&unique_ptr) noexcept
    : RemUniquePtr(unique_ptr.get()) {
  unique_ptr.release();
}

template <typename T> RemUniquePtr<T>::~RemUniquePtr() noexcept { reset(); }

template <typename T>
RemUniquePtr<T>::RemUniquePtr(RemUniquePtr<T> &&o) noexcept
    : RemPtr<T>(std::move(o)) {
  o.release();
}

template <typename T>
RemUniquePtr<T> &RemUniquePtr<T>::operator=(RemUniquePtr<T> &&o) noexcept {
  reset();
  RemPtr<T>::operator=(std::move(o));
  o.release();
  return *this;
}

template <typename T> void RemUniquePtr<T>::release() {
  RemPtr<T>::raw_ptr_ = nullptr;
}

template <typename T> void RemUniquePtr<T>::reset() {
  if (RemPtr<T>::get()) {
    RemPtr<T>::run(get_free_fn<T>());
    release();
  }
}

template <typename T> Future<void> RemUniquePtr<T>::reset_async() {
  if (RemPtr<T>::get()) {
    auto future = RemPtr<T>::run_async(get_free_fn<T>());
    release();
    return future;
  } else {
    release();
    return run_async(+[](T &t) {});
  }
}

template <typename T> void RemUniquePtr<T>::reset_bg() {
  if (RemPtr<T>::get()) {
    // Should allocate from the runtime slab, since the root object might be
    // destructed earlier than this background thread.
    auto *old_heap = Runtime::get_heap();
    Runtime::switch_to_runtime_heap();
    Runtime::rcu_lock.reader_lock();
    rt::Thread([rem_ptr = *static_cast<RemPtr<T> *>(this)]() {
      rem_ptr.run(get_free_fn<T>());
      Runtime::rcu_lock.reader_unlock();
    }).Detach();
    Runtime::set_heap(old_heap);
    release();
  }
}

template <typename T, typename... Args>
RemUniquePtr<T> make_rem_unique(Args &&... args) {
  try {
    return RemUniquePtr<T>(std::make_unique<T>(std::forward<Args>(args)...));
  } catch (std::bad_alloc &) {
    return RemUniquePtr<T>();
  }
}

} // namespace nu

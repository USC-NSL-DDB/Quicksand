#include "nu/runtime.hpp"

namespace nu {

template <typename T> RuntimeDeleter<T>::RuntimeDeleter() noexcept {}

template <typename T>
RuntimeDeleter<T>::RuntimeDeleter(const RuntimeDeleter &o) noexcept {}

template <typename T>
RuntimeDeleter<T> &RuntimeDeleter<T>::
operator=(const RuntimeDeleter &o) noexcept {
  return *this;
}

template <typename T>
RuntimeDeleter<T>::RuntimeDeleter(RuntimeDeleter &&o) noexcept {}

template <typename T>
RuntimeDeleter<T> &RuntimeDeleter<T>::operator=(RuntimeDeleter &&o) noexcept {
  return *this;
}

template <typename T> void RuntimeDeleter<T>::operator()(T *t) noexcept {
  Runtime::delete_on_runtime_heap(t);
}
} // namespace nu

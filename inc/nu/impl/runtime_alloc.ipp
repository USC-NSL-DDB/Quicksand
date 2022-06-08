#include <limits>
#include <type_traits>

#include "nu/runtime.hpp"

namespace nu {

template <typename T>
RuntimeAllocator<T>::RuntimeAllocator() noexcept {}

template <typename T>
template <typename U>
RuntimeAllocator<T>::RuntimeAllocator(const RuntimeAllocator<U> &o) noexcept {}

template <typename T>
template <typename U>
RuntimeAllocator<T> &RuntimeAllocator<T>::operator=(
    const RuntimeAllocator<U> &o) noexcept {
  return *this;
}

template <typename T>
template <typename U>
RuntimeAllocator<T>::RuntimeAllocator(RuntimeAllocator<U> &&o) noexcept {}

template <typename T>
template <typename U>
RuntimeAllocator<T> &RuntimeAllocator<T>::operator=(
    RuntimeAllocator<U> &&o) noexcept {
  return *this;
}

template <typename T>
T *RuntimeAllocator<T>::allocate(std::size_t n) {
  return reinterpret_cast<T *>(Runtime::runtime_slab.allocate(n * sizeof(T)));
}

template <typename T>
void RuntimeAllocator<T>::deallocate(value_type *p, std::size_t n) noexcept {
  Runtime::runtime_slab.free(p);
}

template <typename T>
T *RuntimeAllocator<T>::allocate(std::size_t n, const_void_pointer) {
  return allocate(n);
}

template <typename T>
template <typename U, typename... Args>
void RuntimeAllocator<T>::construct(U *p, Args &&... args) {
  ::new (p) U(std::forward<Args>(args)...);
}

template <typename T>
template <typename U>
void RuntimeAllocator<T>::destroy(U *p) noexcept {
  p->~U();
}

template <typename T>
std::size_t RuntimeAllocator<T>::max_size() const noexcept {
  return std::numeric_limits<size_type>::max();
}

template <class T>
bool operator==(const RuntimeAllocator<T> &x,
                const RuntimeAllocator<T> &y) noexcept {
  return true;
}

template <class T>
bool operator!=(const RuntimeAllocator<T> &x,
                const RuntimeAllocator<T> &y) noexcept {
  return !(x == y);
}

}  // namespace nu

#pragma once

#include <memory>
#include <stack>

#include <sync.h>

#include "defs.hpp"

namespace nu {
template <typename T, typename Allocator = std::allocator<T>> class CachedPool {
public:
  CachedPool(const std::function<T *(void)> &new_fn,
             const std::function<void(T *)> &delete_fn,
             uint32_t per_core_cache_size);
  CachedPool(std::function<T *(void)> &&new_fn,
             std::function<void(T *)> &&delete_fn,
             uint32_t per_core_cache_size);
  ~CachedPool();
  T *get();
  void put(T *item);
  void reserve(uint32_t num);

private:
  using RebindAlloc =
      std::allocator_traits<Allocator>::template rebind_alloc<T *>;
  using ItemStack = std::stack<T *, std::deque<T *, RebindAlloc>>;

  std::function<T *(void)> new_fn_;
  std::function<void(T *)> delete_fn_;
  uint32_t per_core_cache_size_;
  ItemStack locals_[kNumCores];
  ItemStack global_;
  rt::Spin global_spin_;
};
} // namespace nu

#include "impl/cached_pool.ipp"

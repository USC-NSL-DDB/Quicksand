#include <memory>

extern "C" {
#include <runtime/preempt.h>
}

namespace nu {

template <typename T, typename Allocator>
CachedPool<T, Allocator>::CachedPool(const std::function<T *(void)> &new_fn,
                                     const std::function<void(T *)> &delete_fn,
                                     uint32_t per_core_cache_size)
    : new_fn_(new_fn), delete_fn_(delete_fn),
      per_core_cache_size_(per_core_cache_size) {}

template <typename T, typename Allocator>
CachedPool<T, Allocator>::CachedPool(std::function<T *(void)> &&new_fn,
                                     std::function<void(T *)> &&delete_fn,
                                     uint32_t per_core_cache_size)
    : new_fn_(std::move(new_fn)), delete_fn_(std::move(delete_fn)),
      per_core_cache_size_(per_core_cache_size) {}

template <typename T, typename Allocator>
CachedPool<T, Allocator>::~CachedPool() {
  auto drain_stack_fn = [&](auto &stack) {
    while (!stack.empty()) {
      auto *item = stack.top();
      stack.pop();
      delete_fn_(item);
    }
  };

  for (size_t i = 0; i < kNumCores; i++) {
    drain_stack_fn(locals_[i]);
  }
  drain_stack_fn(global_);
}

template <typename T, typename Allocator> T *CachedPool<T, Allocator>::get() {
  int cpu = get_cpu();
  auto &local = locals_[cpu];
  T *item = nullptr;
  if (!local.empty()) {
    item = local.top();
    local.pop();
  }

  if (unlikely(!item)) {
    rt::ScopedLock<rt::Spin> lock(&global_spin_);
    put_cpu();
    while (!global_.empty() && local.size() <= per_core_cache_size_) {
      local.push(global_.top());
      global_.pop();
    }
    while (local.size() <= per_core_cache_size_) {
      local.push(new_fn_());
    }
    item = local.top();
    local.pop();
  } else {
    put_cpu();
  }

  return item;
}

template <typename T, typename Allocator>
void CachedPool<T, Allocator>::put(T *item) {
  int cpu = get_cpu();
  auto &local = locals_[cpu];
  local.push(item);

  if (unlikely(local.size() >= per_core_cache_size_)) {
    rt::ScopedLock<rt::Spin> lock(&global_spin_);
    while (local.size() > per_core_cache_size_ / 2) {
      global_.push(local.top());
      local.pop();
    }
  }
  put_cpu();
}

template <typename T, typename Allocator>
void CachedPool<T, Allocator>::reserve(uint32_t num) {
  auto items = std::make_unique<T *[]>(num);
  for (uint32_t i = 0; i < num; i++) {
    items[i] = new_fn_();
  }

  rt::ScopedLock<rt::Spin> lock(&global_spin_);
  for (uint32_t i = 0; i < num; i++) {
    global_.push(items[i]);
  }
}

} // namespace nu

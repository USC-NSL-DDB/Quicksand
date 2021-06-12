#include <algorithm>

#include "utils/slab.hpp"

namespace nu {

inline void *pop(void **head_p) {
  auto old_head = *head_p;
  *head_p = *reinterpret_cast<void **>(old_head);
  return old_head;
}

inline void push(void **head_p, void *item) {
  auto *old_head = *head_p;
  *head_p = item;
  *reinterpret_cast<void **>(item) = old_head;
}

inline uint32_t get_cache_size(uint32_t size) {
  int cache_size =
      static_cast<int>(SlabAllocator::kMaxCacheSize) -
      size / (SlabAllocator::kCacheSizeCutoff / SlabAllocator::kMaxCacheSize);
  return std::max(1, cache_size);
}

void *SlabAllocator::_allocate(size_t size) noexcept {
  void *ret = nullptr;
  auto slab_shift = get_slab_shift(size);
  if (likely(slab_shift < kMaxSlabClassShift)) {
    int cpu = get_cpu();
    auto &cache = core_caches_[cpu];
    auto &cnt = cache.cnts[slab_shift];
    auto **cached_head = &cache.heads[slab_shift];
    if (cnt) {
      ret = pop(cached_head);
      --cnt;
    }

    if (unlikely(!ret)) {
      rt::ScopedLock<rt::Spin> lock(&spin_);
      auto **slab_head = &slab_heads_[slab_shift];
      auto cache_size = get_cache_size(size);
      while (*slab_head && cnt < cache_size) {
        push(cached_head, pop(slab_head));
        cnt++;
      }

      auto remaining = cache_size - cnt;
      if (remaining) {
        auto slab_size = (1ULL << (slab_shift + 1)) + sizeof(PtrHeader);
        cur_ += slab_size * remaining;
        auto tmp = cur_;
        for (uint32_t i = 0; i < remaining; i++) {
          tmp -= slab_size;
          if (unlikely(tmp + slab_size > end_)) {
            continue;
          }
          push(cached_head, tmp);
          cnt++;
        }
      }

      if (likely(cnt)) {
        ret = pop(cached_head);
        --cnt;
      }
    }
    put_cpu();
  }

  if (ret) {
    auto *hdr = reinterpret_cast<PtrHeader *>(ret);
    hdr->size = size;
    hdr->sentinel = sentinel_;
    ret = reinterpret_cast<uint8_t *>(ret) + sizeof(PtrHeader);
  }

  return ret;
}

void SlabAllocator::_free(const void *_ptr) noexcept {
  auto ptr = const_cast<void *>(_ptr);
  auto *hdr = reinterpret_cast<PtrHeader *>(reinterpret_cast<uintptr_t>(ptr) -
                                            sizeof(PtrHeader));
  auto size = hdr->size;
  BUG_ON(hdr->sentinel != sentinel_);
  ptr = hdr;
  auto slab_shift = get_slab_shift(size);
  if (likely(slab_shift < kMaxSlabClassShift)) {
    int cpu = get_cpu();
    auto &cache = core_caches_[cpu];
    auto &cnt = cache.cnts[slab_shift];
    auto **cached_head = &cache.heads[slab_shift];
    push(cached_head, ptr);
    cnt++;

    auto cache_size = get_cache_size(size);
    if (unlikely(cnt > cache_size)) {
      rt::ScopedLock<rt::Spin> lock(&spin_);
      auto **slab_head = &slab_heads_[slab_shift];
      while (cnt > cache_size / 2 && cnt > 1) {
        push(slab_head, pop(cached_head));
        --cnt;
      }
    }
    put_cpu();
  }
}

void *SlabAllocator::release(size_t size) noexcept {
  rt::ScopedLock<rt::Spin> lock(&spin_);
  if (unlikely(cur_ + size > end_)) {
    return nullptr;
  }
  auto ret = cur_;
  cur_ += size;
  return ret;
}

} // namespace nu

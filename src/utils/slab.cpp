#include <algorithm>

#include "utils/slab.hpp"

namespace nu {

inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("bsr %q1, %q0" : "=r"(ret) : "m"(a));
  return ret;
}

SlabAllocator::SlabAllocator() noexcept {}

SlabAllocator::SlabAllocator(uint16_t sentinel, void *buf,
                             uint64_t len) noexcept {
  init(sentinel, buf, len);
}

SlabAllocator::~SlabAllocator() noexcept {}

void SlabAllocator::init(uint16_t sentinel, void *buf, uint64_t len) noexcept {
  sentinel_ = sentinel;
  start_ = reinterpret_cast<const uint8_t *>(buf);
  end_ = start_ + len;
  cur_ = const_cast<uint8_t *>(start_);
  memset(slab_entries_, 0, sizeof(slab_entries_));
  memset(core_caches_, 0, sizeof(core_caches_));
}

inline uint32_t SlabAllocator::get_slab_shift(uint64_t size) noexcept {
  return std::max(bsr_64(size - 1), kMinSlabClassShift - 1);
}

void *SlabAllocator::allocate(size_t size) noexcept {
  void *ret = nullptr;

  size += sizeof(PtrHeader);
  auto slab_shift = get_slab_shift(size);
  if (likely(slab_shift < kMaxSlabClassShift)) {
    int cpu = get_cpu();
    auto &cache = core_caches_[cpu];
    auto &cnt = cache.cnts[slab_shift];
    auto &cached_entries = cache.entries[slab_shift];
    if (cnt) {
      ret = cached_entries[--cnt];
    }

    if (unlikely(!ret)) {
      rt::ScopedLock<rt::Spin> lock(&spin_);
      auto &slab_head = slab_entries_[slab_shift];
      while (slab_head && cnt <= kPerCoreCacheSize) {
        cached_entries[cnt++] = slab_head;
        slab_head = *reinterpret_cast<void **>(slab_head);
      }
      auto start = cnt;
      while (cnt <= kPerCoreCacheSize) {
        // Other system components assume allocation happens in the FIFO order.
        cached_entries[kPerCoreCacheSize - (cnt++ - start)] = cur_;
        cur_ += (1ULL << (slab_shift + 1));

	if (unlikely(cur_ > end_)) {
	  --cnt;
          if (unlikely(!cnt)) {
            return nullptr;
	  }
          break;
        }
      }
      ret = cached_entries[--cnt];
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

void SlabAllocator::free(const void *_ptr) noexcept {
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
    auto &cached_entries = cache.entries[slab_shift];
    cached_entries[cnt++] = ptr;

    if (unlikely(cnt >= kPerCoreCacheSize)) {
      rt::ScopedLock<rt::Spin> lock(&spin_);
      auto &slab_head = slab_entries_[slab_shift];
      while (cnt > kPerCoreCacheSize / 2) {
        auto old_head = slab_head;
        slab_head = cached_entries[--cnt];
        *reinterpret_cast<void **>(slab_head) = old_head;
      }
    }
    put_cpu();
  }
}

void *SlabAllocator::get_base() const noexcept {
  return const_cast<uint8_t *>(start_);
}

size_t SlabAllocator::get_usage() const noexcept { return cur_ - start_; }

size_t SlabAllocator::get_remaining() const noexcept { return end_ - cur_; }
} // namespace nu

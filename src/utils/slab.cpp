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
  __builtin_memset(slab_entries_, 0, sizeof(slab_entries_));
}

inline uint32_t SlabAllocator::get_slab_shift(uint64_t size) noexcept {
  return std::max(bsr_64(size - 1), kMinSlabClassShift - 1);
}

void *SlabAllocator::allocate(size_t size) noexcept {
  void *ret = nullptr;
  rt::ScopedLock<rt::Spin> lock(&spin_);

  size += sizeof(PtrHeader);
  auto slab_shift = get_slab_shift(size);
  if (likely(slab_shift < kMaxSlabClassShift)) {
    auto &head = slab_entries_[slab_shift];
    if (!head) {
      ret = cur_;
      cur_ += (1ULL << (slab_shift + 1));
      if (unlikely(cur_ > end_)) {
        ret = nullptr;
      }
    } else {
      ret = head;
      head = *reinterpret_cast<void **>(head);
    }
  }

  if (ret) {
    auto *hdr = reinterpret_cast<PtrHeader *>(ret);
    hdr->size = size;
    hdr->sentinel = sentinel_;
    ret = reinterpret_cast<uint8_t *>(ret) + sizeof(PtrHeader);
  }
  return ret;
}

void SlabAllocator::free(const void *ptr) noexcept {
  rt::ScopedLock<rt::Spin> lock(&spin_);

  auto *hdr = reinterpret_cast<PtrHeader *>(reinterpret_cast<uintptr_t>(ptr) -
                                            sizeof(PtrHeader));
  auto size = hdr->size;
  BUG_ON(hdr->sentinel != sentinel_);
  ptr = hdr;
  auto slab_shift = get_slab_shift(size);
  if (likely(slab_shift < kMaxSlabClassShift)) {
    auto &head = slab_entries_[slab_shift];
    auto old_head = head;
    head = const_cast<void *>(ptr);
    *reinterpret_cast<void **>(head) = old_head;
  }
}

void *SlabAllocator::get_base() const noexcept {
  return const_cast<uint8_t *>(start_);
}

size_t SlabAllocator::get_usage() const noexcept { return cur_ - start_; }

size_t SlabAllocator::get_remaining() const noexcept { return end_ - cur_; }
} // namespace nu

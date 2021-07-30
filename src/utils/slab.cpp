#include <algorithm>

#include "nu/utils/slab.hpp"

namespace nu {

SlabAllocator *SlabAllocator::slabs_[std::numeric_limits<SlabId_t>::max() + 1];

void *SlabAllocator::FreePtrsLinkedList::pop() {
  size_--;
  BUG_ON(!head_);
  for (uint32_t i = kBatchSize - 1; i > 0; i--) {
    if (head_->p[i]) {
      auto ret = head_->p[i];
      head_->p[i] = nullptr;
      return ret;
    }
  }
  auto ret = head_;
  head_ = reinterpret_cast<Batch *>(head_->p[0]);
  return ret;
}

void SlabAllocator::FreePtrsLinkedList::push(void *ptr) {
  size_++;
  if (unlikely(!head_)) {
    head_ = reinterpret_cast<Batch *>(ptr);
    std::fill(std::begin(head_->p), std::end(head_->p), nullptr);
    return;
  }

  for (uint32_t i = 1; i < kBatchSize; i++) {
    if (!head_->p[i]) {
      head_->p[i] = ptr;
      return;
    }
  }
  auto old_head = head_;
  head_ = reinterpret_cast<Batch *>(ptr);
  head_->p[0] = old_head;
  std::fill(std::begin(head_->p) + 1, std::end(head_->p), nullptr);
}

// TODO: should be dynamic.
inline uint32_t get_num_cache_entries(uint32_t slab_shift) {
  switch (slab_shift) {
  case 4: // 32 B
    return 64;
  case 5: // 64 B
    return 64;
  case 6: // 128 B
    return 32;
  case 7: // 256 B
    return 32;
  case 8: // 512 B
    return 16;
  case 9: // 1024 B
    return 8;
  case 10: // 2048 B
    return 4;
  default:
    return 1;
  }
}

void *SlabAllocator::_allocate(size_t size) noexcept {
  void *ret = nullptr;
  auto slab_shift = get_slab_shift(size);
  if (likely(slab_shift < kMaxSlabClassShift)) {
    int cpu = get_cpu();
    auto &cache_list = cache_lists_[cpu].lists[slab_shift];
    if (likely(cache_list.size())) {
      ret = cache_list.pop();
    }

    if (unlikely(!ret)) {
      rt::ScopedLock<rt::Spin> lock(&spin_);
      auto &slab_list = slab_lists_[slab_shift];
      auto num_cache_entries = get_num_cache_entries(slab_shift);
      while (slab_list.size() && cache_list.size() < num_cache_entries) {
        cache_list.push(slab_list.pop());
      }

      auto remaining = num_cache_entries - cache_list.size();
      if (remaining) {
        auto slab_size = (1ULL << (slab_shift + 1)) + sizeof(PtrHeader);
        cur_ += slab_size * remaining;
        auto tmp = cur_;
        for (uint32_t i = 0; i < remaining; i++) {
          tmp -= slab_size;
          if (unlikely(tmp + slab_size > end_)) {
            continue;
          }
	  cache_list.push(tmp);
        }
      }

      if (likely(cache_list.size())) {
        ret = cache_list.pop();
      }
    }
    put_cpu();
  }

  if (ret) {
    auto *hdr = reinterpret_cast<PtrHeader *>(ret);
    hdr->size = size;
    hdr->slab_id = slab_id_;
    ret = reinterpret_cast<uint8_t *>(ret) + sizeof(PtrHeader);
  }

  return ret;
}

void SlabAllocator::_free(const void *_ptr) noexcept {
  auto ptr = const_cast<void *>(_ptr);
  auto *hdr = reinterpret_cast<PtrHeader *>(reinterpret_cast<uintptr_t>(ptr) -
                                            sizeof(PtrHeader));
  auto *slab = slabs_[hdr->slab_id];
  assert(reinterpret_cast<const uint8_t *>(_ptr) < slab->start_);
  assert(reinterpret_cast<const uint8_t *>(_ptr) >= slab->cur_);

  auto size = hdr->size;
  ptr = hdr;
  auto slab_shift = slab->get_slab_shift(size);
  if (likely(slab_shift < slab->kMaxSlabClassShift)) {
    int cpu = get_cpu();
    auto &cache_list = slab->cache_lists_[cpu].lists[slab_shift];
    cache_list.push(ptr);

    auto num_cache_entries = get_num_cache_entries(slab_shift);
    if (unlikely(cache_list.size() > num_cache_entries)) {
      rt::ScopedLock<rt::Spin> lock(&slab->spin_);
      auto &slab_list = slab->slab_lists_[slab_shift];
      while (cache_list.size() > num_cache_entries / 2 &&
             cache_list.size() > 1) {
	slab_list.push(cache_list.pop());
      }
    }
    put_cpu();
  }
}

void *SlabAllocator::yield(size_t size) noexcept {
  rt::ScopedLock<rt::Spin> lock(&spin_);
  if (unlikely(cur_ + size > end_)) {
    return nullptr;
  }
  auto ret = cur_;
  cur_ += size;
  return ret;
}

} // namespace nu

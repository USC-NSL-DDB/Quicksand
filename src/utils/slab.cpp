#include <algorithm>

#include "nu/utils/slab.hpp"

namespace nu {

SlabAllocator *SlabAllocator::slabs_[std::numeric_limits<SlabId_t>::max()];

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
    memset(head_->p, 0, sizeof(Batch));
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

inline uint32_t get_min_num_cache_entries(uint32_t size_shift) {
  constexpr uint32_t num_bound = 64;
  constexpr uint32_t size_bound = 8192;
  return std::max(static_cast<uint32_t>(1),
                  std::min(num_bound, size_bound >> size_shift));
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
      auto min_num_cache_entries = get_min_num_cache_entries(slab_shift);
      while (slab_list.size() && cache_list.size() < min_num_cache_entries) {
        cache_list.push(slab_list.pop());
      }

      auto remaining = min_num_cache_entries - cache_list.size();
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
  auto size = hdr->size;

  ptr = hdr;
  auto slab_shift = slab->get_slab_shift(size);
  if (likely(slab_shift < slab->kMaxSlabClassShift)) {
    int cpu = get_cpu();
    auto &cache_list = slab->cache_lists_[cpu].lists[slab_shift];
    cache_list.push(ptr);
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

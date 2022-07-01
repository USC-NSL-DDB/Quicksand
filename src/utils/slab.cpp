#include <algorithm>

#include "nu/utils/slab.hpp"

namespace nu {

SlabAllocator *SlabAllocator::slabs_[get_max_slab_id() + 1];

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
    case 4:  // 32 B
      return 64;
    case 5:  // 64 B
      return 64;
    case 6:  // 128 B
      return 32;
    case 7:  // 256 B
      return 32;
    case 8:  // 512 B
      return 16;
    case 9:  // 1024 B
      return 8;
    case 10:  // 2048 B
      return 4;
    case 11:  // 4096 B
      return 2;
    case 12:  // 8192 B
      return 1;
    default:
      return 0;
  }
}

#ifdef SLAB_TRANSFER_CACHE
inline void SlabAllocator::drain_transferred_cache(
    const rt::Preempt &p, uint32_t slab_shift) noexcept {
  auto &transferred_cache = transferred_caches_[p.get_cpu()];
  auto &list = transferred_cache.lists[slab_shift];

  if (list.size()) {
    rt::SpinGuard g(&transferred_cache.spin);

    while (list.size()) {
      auto *ptr = list.pop();
      __do_free(p, ptr, slab_shift);
    }
  }
}
#endif

void *SlabAllocator::__allocate(size_t size) noexcept {
  void *ret = nullptr;
  int cpu;
  auto slab_shift = get_slab_shift(size);

  if (likely(slab_shift < kMaxSlabClassShift)) {
    rt::Preempt p;
    rt::PreemptGuard g(&p);

#ifdef SLAB_TRANSFER_CACHE
    drain_transferred_cache(p, slab_shift);
#endif

    cpu = p.get_cpu();
    auto &cache_list = cache_lists_[cpu].lists[slab_shift];
    if (likely(cache_list.size())) {
      ret = cache_list.pop();
    }

    if (unlikely(!ret)) {
      rt::SpinGuard g(&spin_);
      auto &slab_list = slab_lists_[slab_shift];
      auto num_cache_entries =
          std::max(static_cast<uint32_t>(1), get_num_cache_entries(slab_shift));
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
  }

  if (ret) {
    auto *hdr = reinterpret_cast<PtrHeader *>(ret);
    hdr->size = size;
#ifdef SLAB_TRANSFER_CACHE
    hdr->core_id = cpu;
#endif
    hdr->slab_id = slab_id_;
    auto addr = reinterpret_cast<uintptr_t>(ret);
    addr += sizeof(PtrHeader);
    assert(addr % kAlignment == 0);
    ret = reinterpret_cast<uint8_t *>(addr);
  }

  return ret;
}

void SlabAllocator::__free(const void *_ptr) noexcept {
  auto ptr = const_cast<void *>(_ptr);
  auto *hdr = reinterpret_cast<PtrHeader *>(reinterpret_cast<uintptr_t>(ptr) -
                                            sizeof(PtrHeader));
  auto *slab = slabs_[hdr->slab_id];
  assert(reinterpret_cast<const uint8_t *>(_ptr) >= slab->start_);
  assert(reinterpret_cast<const uint8_t *>(_ptr) < slab->cur_);

  auto size = hdr->size;
  ptr = hdr;
  auto slab_shift = slab->get_slab_shift(size);

  if (likely(slab_shift < slab->kMaxSlabClassShift)) {
    rt::Preempt p;
    rt::PreemptGuard g(&p);

#ifdef SLAB_TRANSFER_CACHE
    slab->drain_transferred_cache(p, slab_shift);
    if (likely(p.get_cpu() == hdr->core_id)) {
#endif
      slab->__do_free(p, ptr, slab_shift);
#ifdef SLAB_TRANSFER_CACHE
    } else {
      auto &transferred_cache = slab->transferred_caches_[hdr->core_id];
      rt::SpinGuard g(&transferred_cache.spin);
      transferred_cache.lists[slab_shift].push(ptr);
    }
#endif
  }
}

inline void SlabAllocator::__do_free(const rt::Preempt &p, void *ptr,
                                     uint32_t slab_shift) noexcept {
  auto &cache_list = cache_lists_[p.get_cpu()].lists[slab_shift];
  cache_list.push(ptr);

  auto num_cache_entries = get_num_cache_entries(slab_shift);
  if (unlikely(cache_list.size() > num_cache_entries)) {
    rt::SpinGuard g(&spin_);
    auto &slab_list = slab_lists_[slab_shift];
    while (cache_list.size() > num_cache_entries / 2) {
      slab_list.push(cache_list.pop());
    }
  }
}

void *SlabAllocator::yield(size_t size) noexcept {
  rt::SpinGuard g(&spin_);
  size = (((size - 1) / kAlignment) + 1) * kAlignment;
  if (unlikely(cur_ + size > end_)) {
    return nullptr;
  }
  auto ret = cur_;
  cur_ += size;
  return ret;
}

}  // namespace nu

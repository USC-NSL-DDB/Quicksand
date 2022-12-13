#include <cstring>
#include <iostream>

namespace nu {

inline SlabAllocator::SlabAllocator() noexcept : slab_id_(0) {}

inline SlabAllocator::SlabAllocator(SlabId_t slab_id, void *buf, uint64_t len,
                                    bool aggressive_caching) noexcept {
  init(slab_id, buf, len, aggressive_caching);
}

inline SlabAllocator::~SlabAllocator() noexcept {
  if (slab_id_) {
    deregister_slab_by_id(slab_id_);
  }
}

inline void SlabAllocator::init(SlabId_t slab_id, void *buf, uint64_t len,
                                bool aggressive_caching) noexcept {
  register_slab_by_id(this, slab_id);
  slab_id_ = slab_id;
  aggressive_caching_ = aggressive_caching;
  start_ = reinterpret_cast<const uint8_t *>(buf);
  end_ = const_cast<uint8_t *>(start_) + len;
  cur_ = const_cast<uint8_t *>(start_);
  global_free_bytes_ = 0;
}

inline void *SlabAllocator::allocate(size_t size) noexcept {
  if (unlikely(!size)) {
    return nullptr;
  }
  return __allocate(size);
}

inline void SlabAllocator::free(const void *ptr) noexcept {
  if (unlikely(!ptr)) {
    return;
  }
  __free(ptr);
}

inline uint32_t SlabAllocator::get_slab_shift(uint64_t data_size) noexcept {
  return std::max(bsr_64(data_size - 1), kMinSlabClassShift - 1);
}

inline uint64_t SlabAllocator::get_slab_size(uint32_t slab_shift) noexcept {
  return (1ULL << (slab_shift + 1)) + sizeof(PtrHeader);
}

inline void *SlabAllocator::get_base() const noexcept {
  return const_cast<uint8_t *>(start_);
}

inline size_t SlabAllocator::get_usage() const noexcept {
  std::size_t ever_allocated = rt::access_once(cur_) - start_;
  auto global_free_bytes =
      static_cast<std::size_t>(rt::access_once(global_free_bytes_));
  BUG_ON(ever_allocated < global_free_bytes);
  return ever_allocated - global_free_bytes;
}

inline size_t SlabAllocator::get_remaining() const noexcept {
  return end_ - start_ - get_usage();
}

inline bool SlabAllocator::try_shrink(size_t new_len) noexcept {
  if (cur_ > new_len + start_) {
    return false;
  }
  end_ = const_cast<uint8_t *>(start_) + new_len;
  return true;
}

inline SlabId_t SlabAllocator::get_id() noexcept { return slab_id_; }

inline void SlabAllocator::register_slab_by_id(SlabAllocator *slab,
                                               SlabId_t slab_id) noexcept {
  BUG_ON(slabs_[slab_id]);
  slabs_[slab_id] = slab;
}

inline void SlabAllocator::deregister_slab_by_id(SlabId_t slab_id) noexcept {
  BUG_ON(!slabs_[slab_id]);
  slabs_[slab_id] = nullptr;
}

inline uint64_t SlabAllocator::FreePtrsLinkedList::size() { return size_; }

}  // namespace nu

#include <cstring>
#include <iostream>

namespace nu {

inline SlabAllocator::SlabAllocator() noexcept {}

inline SlabAllocator::SlabAllocator(SlabId_t slab_id, void *buf,
                                    uint64_t len) noexcept {
  init(slab_id, buf, len);
}

inline SlabAllocator::~SlabAllocator() noexcept {}

inline void SlabAllocator::init(SlabId_t slab_id, void *buf,
                                uint64_t len) noexcept {
  register_slab_by_id(this, slab_id);
  slab_id_ = slab_id;
  start_ = reinterpret_cast<const uint8_t *>(buf);
  end_ = const_cast<uint8_t *>(start_) + len;
  cur_ = const_cast<uint8_t *>(start_);
}

inline void *SlabAllocator::allocate(size_t size) noexcept {
  if (unlikely(!size)) {
    return nullptr;
  }
  return _allocate(size);
}

inline void SlabAllocator::free(const void *ptr) noexcept {
  if (unlikely(!ptr)) {
    return;
  }
  _free(ptr);
}

inline uint32_t SlabAllocator::get_slab_shift(uint64_t size) noexcept {
  return std::max(bsr_64(size - 1), kMinSlabClassShift - 1);
}

inline void *SlabAllocator::get_base() const noexcept {
  return const_cast<uint8_t *>(start_);
}

inline size_t SlabAllocator::get_usage() const noexcept {
  return ACCESS_ONCE(cur_) - start_;
}

inline size_t SlabAllocator::get_remaining() const noexcept {
  return end_ - ACCESS_ONCE(cur_);
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

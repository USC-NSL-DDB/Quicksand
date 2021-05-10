namespace nu {

inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("bsr %q1, %q0" : "=r"(ret) : "m"(a));
  return ret;
}
  
inline SlabAllocator::SlabAllocator() noexcept {}

inline SlabAllocator::SlabAllocator(uint16_t sentinel, void *buf,
                                    uint64_t len) noexcept {
  init(sentinel, buf, len);
}

inline SlabAllocator::~SlabAllocator() noexcept {}

inline void SlabAllocator::init(uint16_t sentinel, void *buf,
                                uint64_t len) noexcept {
  sentinel_ = sentinel;
  start_ = reinterpret_cast<const uint8_t *>(buf);
  end_ = start_ + len;
  cur_ = const_cast<uint8_t *>(start_);
  memset(slab_heads_, 0, sizeof(slab_heads_));
  memset(core_caches_, 0, sizeof(core_caches_));
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
  return cur_ - start_;
}

inline size_t SlabAllocator::get_remaining() const noexcept {
  return end_ - cur_;
}

} // namespace nu

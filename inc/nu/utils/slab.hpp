#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <stack>
#include <string>
#include <string_view>
#include <type_traits>

#include "nu/commons.hpp"
#include "nu/utils/caladan.hpp"
#include "nu/utils/spin_lock.hpp"

namespace nu {

#ifdef SLAB_TRANSFER_CACHE
struct PtrHeader {
  uint64_t size : 56;
  uint64_t core_id : 8;
  SlabId_t slab_id;
};
#else
struct PtrHeader {
  uint64_t size;
  SlabId_t slab_id;
};
#endif

// It's the constraint placed by GCC for enabling vectorization optimizations.
constexpr static uint32_t kAlignment = 16;
static_assert(sizeof(PtrHeader) % kAlignment == 0);

class SlabAllocator {
 public:
  constexpr static uint64_t kMaxSlabClassShift = 35;  // 32 GB.
  constexpr static uint64_t kMinSlabClassShift = 5;   // 32 B.
  constexpr static uint64_t kMaxNumCacheEntries = 32;
  constexpr static uint64_t kCacheSizeCutoff = 1024;
  static_assert((1 << kMinSlabClassShift) % kAlignment == 0);

  SlabAllocator() noexcept;
  SlabAllocator(SlabId_t slab_id, void *buf, size_t len,
                bool aggressive_caching = false) noexcept;
  ~SlabAllocator() noexcept;
  void init(SlabId_t slab_id, void *buf, size_t len,
            bool aggressive_caching = false) noexcept;
  void *allocate(size_t size) noexcept;
  void *yield(size_t size) noexcept;
  void *get_base() const noexcept;
  size_t get_usage() const noexcept;
  size_t get_remaining() const noexcept;
  bool try_shrink(size_t new_len) noexcept;
  SlabId_t get_id() noexcept;
  static SlabAllocator *get_slab_by_id() noexcept;
  static void free(const void *ptr) noexcept;
  static void register_slab_by_id(SlabAllocator *slab,
                                  SlabId_t slab_id) noexcept;
  static void deregister_slab_by_id(SlabId_t slab_id) noexcept;

 private:
  class FreePtrsLinkedList {
   public:
    void push(void *ptr);
    void *pop();
    uint64_t size();

   private:
    constexpr static uint32_t kBatchSize =
        ((1 << kMinSlabClassShift) + sizeof(PtrHeader)) / sizeof(void *);
    struct Batch {
      void *p[kBatchSize];
    };

    Batch *head_ = nullptr;
    uint64_t size_ = 0;
  };

  struct alignas(kCacheLineBytes) CoreCache {
    FreePtrsLinkedList lists[kMaxSlabClassShift];
  };

  struct alignas(kCacheLineBytes) TransferredCoreCache {
    SpinLock spin;
    FreePtrsLinkedList lists[kMaxSlabClassShift];
  };

  static SlabAllocator *slabs_[get_max_slab_id() + 1];
  SlabId_t slab_id_;
  bool aggressive_caching_;
  const uint8_t *start_;
  uint8_t *end_;
  uint8_t *cur_;
  FreePtrsLinkedList slab_lists_[kMaxSlabClassShift];
  CoreCache cache_lists_[kNumCores];
#ifdef SLAB_TRANSFER_CACHE
  TransferredCoreCache transferred_caches_[kNumCores];
#endif
  SpinLock spin_;

  void *__allocate(size_t size) noexcept;
  static void __free(const void *ptr) noexcept;
  void __do_free(const Caladan::PreemptGuard &g, void *ptr,
                 uint32_t slab_shift) noexcept;
  uint32_t get_slab_shift(uint64_t size) noexcept;
  void drain_transferred_cache(const Caladan::PreemptGuard &g,
                               uint32_t slab_shift) noexcept;
};
}  // namespace nu

#include "nu/impl/slab.ipp"

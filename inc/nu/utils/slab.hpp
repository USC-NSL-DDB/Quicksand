#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <stack>
#include <string>
#include <string_view>
#include <type_traits>

extern "C" {
#include <base/compiler.h>
#include <runtime/preempt.h>
}
#include <sync.h>

#include "nu/commons.hpp"

namespace nu {

struct PtrHeader {
  static_assert(sizeof(SlabId_t) < sizeof(uint64_t));
  uint64_t size : 8 * (sizeof(uint64_t) - sizeof(SlabId_t));
  uint64_t slab_id : 8 * sizeof(SlabId_t);
};

class SlabAllocator {
public:
  constexpr static uint64_t kMaxSlabClassShift = 35; // 32 GB.
  constexpr static uint64_t kMinSlabClassShift = 5;  // 32 B.
  constexpr static uint64_t kMaxNumCacheEntries = 32;
  constexpr static uint64_t kCacheSizeCutoff = 1024;

  SlabAllocator() noexcept;
  SlabAllocator(SlabId_t slab_id, void *buf, size_t len) noexcept;
  ~SlabAllocator() noexcept;
  void init(SlabId_t slab_id, void *buf, size_t len) noexcept;
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
        (1 << kMinSlabClassShift) / sizeof(void *);
    struct Batch {
      void *p[kBatchSize];
    };

    Batch *head_ = nullptr;
    uint64_t size_ = 0;
  };

  struct alignas(kCacheLineBytes) CoreCache {
    FreePtrsLinkedList lists[kMaxSlabClassShift];
  };

  static SlabAllocator *slabs_[std::numeric_limits<SlabId_t>::max() + 1];
  SlabId_t slab_id_;
  const uint8_t *start_;
  uint8_t *end_;
  uint8_t *cur_;
  FreePtrsLinkedList slab_lists_[kMaxSlabClassShift];
  CoreCache cache_lists_[kNumCores];
  rt::Spin spin_;

  void *_allocate(size_t size) noexcept;
  static void _free(const void *ptr) noexcept;
  uint32_t get_slab_shift(uint64_t size) noexcept;
};
} // namespace nu

#include "nu/impl/slab.ipp"

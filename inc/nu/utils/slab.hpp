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

using SlabId_t = uint16_t;

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
  SlabAllocator(uint16_t sentinel, void *buf, size_t len) noexcept;
  ~SlabAllocator() noexcept;
  void init(uint16_t sentinel, void *buf, size_t len) noexcept;
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

private:
  struct alignas(kCacheLineBytes) CoreCache {
    using CntType = uint8_t;
    CntType cnts[kMaxSlabClassShift];
    void *heads[kMaxSlabClassShift];
  };
  static_assert(std::numeric_limits<CoreCache::CntType>::max() >=
                kMaxNumCacheEntries);

  static SlabAllocator *slabs_[std::numeric_limits<SlabId_t>::max()];
  uint16_t slab_id_;
  const uint8_t *start_;
  uint8_t *end_;
  uint8_t *cur_;
  void *slab_heads_[kMaxSlabClassShift];
  CoreCache core_caches_[kNumCores];
  rt::Spin spin_;

  void *_allocate(size_t size) noexcept;
  static void _free(const void *ptr) noexcept;
  uint32_t get_slab_shift(uint64_t size) noexcept;
};
} // namespace nu

#include "nu/impl/slab.ipp"

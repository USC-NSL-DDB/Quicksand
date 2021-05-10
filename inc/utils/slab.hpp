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

#include "defs.hpp"

namespace nu {

struct PtrHeader {
  uint64_t size : 48;
  uint64_t sentinel : 16;
};

class SlabAllocator {
public:
  constexpr static uint64_t kMaxSlabClassShift = 32; // 4 GB.
  constexpr static uint64_t kMinSlabClassShift = 5;  // 32 B.
  constexpr static uint64_t kMaxCacheSize = 32;
  constexpr static uint64_t kCacheSizeCutoff = 1024;

  SlabAllocator() noexcept;
  SlabAllocator(uint16_t sentinel, void *buf, size_t len) noexcept;
  ~SlabAllocator() noexcept;
  void init(uint16_t sentinel, void *buf, size_t len) noexcept;
  void *allocate(size_t size) noexcept;
  void free(const void *ptr) noexcept;
  void *get_base() const noexcept;
  size_t get_usage() const noexcept;
  size_t get_remaining() const noexcept;

private:
  struct alignas(kCacheLineBytes) CoreCache {
    using CntType = uint8_t;
    CntType cnts[kMaxSlabClassShift];
    void *heads[kMaxSlabClassShift];
  };
  static_assert(std::numeric_limits<CoreCache::CntType>::max() >=
                kMaxCacheSize);

  uint16_t sentinel_;
  const uint8_t *start_;
  const uint8_t *end_;
  uint8_t *cur_;
  void *slab_heads_[kMaxSlabClassShift];
  CoreCache core_caches_[kNumCores];
  rt::Spin spin_;

  void *_allocate(size_t size) noexcept;
  void _free(const void *ptr) noexcept;
  uint32_t get_slab_shift(uint64_t size) noexcept;
};
} // namespace nu

#include "impl/slab.ipp"

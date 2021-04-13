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
  constexpr static uint64_t kPerCoreCacheSize = 31;

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
    // TODO: make these as linkedlists.
    void *entries[kMaxSlabClassShift][kPerCoreCacheSize + 1];
  };
  static_assert(std::numeric_limits<CoreCache::CntType>::max() >=
                kPerCoreCacheSize);

  uint16_t sentinel_;
  const uint8_t *start_;
  const uint8_t *end_;
  uint8_t *cur_;
  void *slab_entries_[kMaxSlabClassShift];
  CoreCache core_caches_[kNumCores];
  rt::Spin spin_;

  uint32_t get_slab_shift(uint64_t size) noexcept;
};
} // namespace nu

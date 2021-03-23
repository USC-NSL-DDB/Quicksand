#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>

extern "C" {
#include <base/compiler.h>
}
#include "sync.h"

// TODO: add per-core cache.

namespace nu {

struct PtrHeader {
  constexpr static uint16_t kSentinel = 0xBE;
  uint64_t size : 48;
  uint64_t sentinel : 16;
};

class SlabAllocator {
public:
  constexpr static uint64_t kMaxSlabClassShift = 35; // 32 GB.
  constexpr static uint64_t kMinSlabClassShift = 5;  // 32 B.

  SlabAllocator() noexcept;
  SlabAllocator(void *buf, size_t len) noexcept;
  ~SlabAllocator() noexcept;
  void init(void *buf, size_t len) noexcept;
  void *allocate(size_t size) noexcept;
  void free(const void *ptr) noexcept;
  void *get_base() const noexcept;
  size_t get_usage() const noexcept;
  size_t get_remaining() const noexcept;

private:
  rt::Spin spin_;
  const uint8_t *start_;
  const uint8_t *end_;
  uint8_t *cur_;
  void *slab_entries_[kMaxSlabClassShift];

  uint32_t get_slab_shift(uint64_t size) noexcept;
};
} // namespace nu

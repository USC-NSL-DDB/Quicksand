#pragma once

namespace nu {

constexpr static uint32_t kNumCores = 48;
constexpr static uint32_t kCacheLineBytes = 64;

using RemObjID = uint64_t;
inline void *to_heap_base(RemObjID id) { return reinterpret_cast<void *>(id); }
} // namespace nu

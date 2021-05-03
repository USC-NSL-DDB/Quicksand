#pragma once

namespace nu {

struct Resource {
  uint32_t cores;
  uint32_t mem_mbs;

  bool empty() const { return cores == 0 && mem_mbs == 0; }
};

#ifndef NCORES
#error Must indicate number of CPU cores
#endif

using RemObjID = uint64_t;

constexpr static RemObjID kNullRemObjID = 0;
constexpr static uint64_t kNumCores = NCORES;
constexpr static uint64_t kCacheLineBytes = 64;
constexpr static uint64_t kPtrHeaderSize = 8;
constexpr static uint64_t kStackAlignment = 16;
constexpr static uint64_t kStackSize =
    (32 << 10) - kPtrHeaderSize - kStackAlignment;

inline void *to_heap_base(RemObjID id) { return reinterpret_cast<void *>(id); }
inline RemObjID to_obj_id(void *heap_base) {
  return reinterpret_cast<RemObjID>(heap_base);
}
} // namespace nu

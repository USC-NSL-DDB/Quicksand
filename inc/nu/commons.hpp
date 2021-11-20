#pragma once

extern "C" {
#include <runtime/thread.h>
}

#include <cstddef>
#include <span>

namespace nu {

#ifndef NCORES
#error Must indicate number of CPU cores
#endif

struct HeapHeader;

struct Resource {
  uint32_t cores;
  uint32_t mem_mbs;
};

struct VAddrRange {
  uint64_t start;
  uint64_t end;

  bool operator<(const VAddrRange &o) const {
    return std::make_pair(start, end) < std::make_pair(o.start, o.end);
  }
};

struct HeapRange {
  HeapHeader *heap_header;
  uint64_t len;
};

union WaiterInfo {
  struct {
    uint64_t type : 16;
    uint64_t addr : 48;
  };
  uint64_t raw;
};

enum WaiterType { kNone = 0, kMutex, kCondVar, kTimer };

struct ErasedType {};

using RemObjID = uint64_t;

constexpr static uint64_t kNumCores = NCORES;
constexpr static uint64_t kCacheLineBytes = 64;
constexpr static uint64_t kStackAlignment = 16;
constexpr static uint64_t kStackRedZoneSize = 128;
constexpr static uint64_t kStackSize = 64ULL << 10;
constexpr static uint64_t kPageSize = 4096;

constexpr static RemObjID kNullRemObjID = 0;

// TODO: double check.
constexpr static uint64_t kMinHeapVAddr = 0x300000000000ULL;
constexpr static uint64_t kMaxHeapVAddr = 0x400000000000ULL;
constexpr static uint64_t kHeapSize = 0x40000000ULL;
constexpr static uint64_t kMinStackClusterVAddr = kMaxHeapVAddr;
constexpr static uint64_t kMaxStackClusterVAddr = 0x500000000000ULL;
constexpr static uint64_t kStackClusterSize = 1ULL << 30;
constexpr static uint64_t kMaxNumStacksPerCluster =
    kStackClusterSize / kStackSize;
constexpr static uint64_t kMinRuntimeHeapVaddr = kMaxStackClusterVAddr;
constexpr static uint64_t kRuntimeHeapSize = 128ULL << 30;
constexpr static uint16_t kRuntimeSlabId = 1;

constexpr static uint64_t kOneMB = 1ULL << 20;
constexpr static uint64_t kOneSecond = 1000 * 1000;
constexpr static uint64_t kOneMilliSecond = 1000;

uint64_t bsr_64(uint64_t a);
HeapHeader *to_heap_header(RemObjID id);
void *to_heap_base(RemObjID id);
RemObjID to_obj_id(void *heap_base);
void *switch_to_obj_stack(void *stack);
void switch_to_runtime_stack(void *old_rsp);
VAddrRange get_obj_stack_range(thread_t *thread);
uint16_t to_u16(void *heap_base);
template <typename T> std::span<std::byte> to_span(T &t);
template <typename T> T &from_span(std::span<std::byte> span);
template <typename T> const T &from_span(std::span<const std::byte> span);

} // namespace nu

#include "nu/impl/commons.ipp"

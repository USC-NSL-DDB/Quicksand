#pragma once

extern "C" {
#include <runtime/thread.h>
}

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
};

struct HeapRange {
  HeapHeader *heap_header;
  uint64_t len;
};

struct ErasedType {};

using RemObjID = uint64_t;
using SlabId_t = uint16_t;

constexpr static uint64_t kNumCores = NCORES;
constexpr static uint64_t kCacheLineBytes = 64;
constexpr static uint64_t kStackAlignment = 16;
constexpr static uint64_t kStackRedZoneSize = 128;
constexpr static uint64_t kStackSize = 256ULL << 10;
constexpr static uint64_t kPageSize = 4096;

constexpr static RemObjID kNullRemObjID = 0;

constexpr static uint64_t kMinHeapVAddr = 0x80000000ULL;
constexpr static uint64_t kMaxHeapVAddr = 0x400000000000ULL;
constexpr static uint64_t kHeapSize = 0x40000000ULL;
constexpr static uint64_t kMinStackClusterVAddr = kMaxHeapVAddr;
constexpr static uint64_t kMaxStackClusterVAddr = 0x500000000000ULL;
constexpr static uint64_t kStackClusterSize = 1ULL << 30;
constexpr static uint64_t kMaxNumStacksPerCluster =
    kStackClusterSize / kStackSize;
constexpr static uint64_t kMinRuntimeHeapVaddr = kMaxStackClusterVAddr;
constexpr static uint64_t kRuntimeHeapSize = 48ULL << 30;
constexpr static SlabId_t kRuntimeSlabId = 1;

constexpr static uint64_t kOneMB = 1ULL << 20;
constexpr static uint64_t kOneSecond = 1000 * 1000;
constexpr static uint64_t kOneMilliSecond = 1000;

uint64_t bsr_64(uint64_t a);
void *to_heap_base(RemObjID id);
RemObjID to_obj_id(void *heap_base);
void *switch_to_obj_stack(void *stack);
void switch_to_runtime_stack(void *old_rsp);
VAddrRange get_obj_stack_range(thread_t *thread);
SlabId_t to_slab_id(void *heap_base);

} // namespace nu

#include "nu/impl/commons.ipp"

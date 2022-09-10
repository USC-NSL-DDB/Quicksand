#pragma once

extern "C" {
#include <runtime/thread.h>
}

#include <cstddef>
#include <span>
#include <string>

namespace nu {

#ifndef NCORES
#error Must indicate number of CPU cores
#endif

using ProcletID = uint64_t;
using lpid_t = uint16_t;
using SlabId_t = uint32_t;
using NodeIP = uint32_t;

struct ErasedType {};
struct ProcletHeader;

struct Resource {
  float cores;
  float mem_mbs;
};

struct VAddrRange {
  uint64_t start;
  uint64_t end;

  bool operator<(const VAddrRange &o) const {
    return std::make_pair(start, end) < std::make_pair(o.start, o.end);
  }
};

template <typename T>
union MethodPtr {
  T ptr;
  uint8_t raw[sizeof(T)];

  template <class Archive>
  void serialize(Archive &ar) {
    ar(raw);
  }
};

constexpr static uint64_t kNumCores = NCORES;
constexpr static uint64_t kCacheLineBytes = 64;
constexpr static uint64_t kStackAlignment = 16;
constexpr static uint64_t kStackRedZoneSize = 128;
constexpr static uint64_t kStackSize = 64ULL << 10;
constexpr static uint64_t kPageSize = 4096;

constexpr static ProcletID kNullProcletID = 0;

// TODO: double check.
constexpr static uint64_t kMinProcletHeapVAddr = 0x300000000000ULL;
constexpr static uint64_t kMaxProcletHeapVAddr = 0x400000000000ULL;
constexpr static uint64_t kMinProcletHeapSize = 1ULL << 25;
constexpr static uint64_t kMaxProcletHeapSize = 1ULL << 30;
constexpr static uint64_t kMaxNumProclets =
    (kMaxProcletHeapVAddr - kMinProcletHeapVAddr) / kMinProcletHeapSize;
constexpr static uint64_t kMinStackClusterVAddr = kMaxProcletHeapVAddr;
constexpr static uint64_t kMaxStackClusterVAddr = 0x600000000000ULL;
constexpr static uint64_t kStackClusterSize = 1ULL << 31;
constexpr static uint64_t kMaxNumStacksPerCluster =
    kStackClusterSize / kStackSize;
constexpr static uint64_t kMinRuntimeHeapVaddr = kMaxStackClusterVAddr;
constexpr static uint64_t kRuntimeHeapSize = 128ULL << 30;
constexpr static uint32_t kRuntimeSlabId = 1;

constexpr static uint64_t kOneMB = 1ULL << 20;
constexpr static uint64_t kOneSecond = 1000 * 1000;
constexpr static uint64_t kOneMilliSecond = 1000;

constexpr uint64_t bsr_64(uint64_t a);
constexpr ProcletHeader *to_proclet_header(ProcletID id);
constexpr void *to_proclet_base(ProcletID id);
constexpr ProcletID to_proclet_id(void *proclet_base);
constexpr SlabId_t to_slab_id(void *proclet_base);
constexpr SlabId_t to_slab_id(uint64_t proclet_base_addr);
constexpr SlabId_t get_max_slab_id();
void *switch_stack(void *new_rsp);
VAddrRange get_proclet_stack_range(thread_t *thread);
bool is_copied_on_migration(void *ptr, ProcletHeader *proclet_header);
template <typename T>
std::span<std::byte> to_span(T &t);
template <typename T>
T &from_span(std::span<std::byte> span);
template <typename T>
const T &from_span(std::span<const std::byte> span);
uint32_t str_to_ip(std::string ip_str);
void unblock_and_relax();
template <typename T>
constexpr T div_round_up_unchecked(T dividend, T divisor);
constexpr uint64_t round_up_to_power2(uint64_t x);

template <class T>
concept BoolIntegral = requires {
  requires std::is_same_v<T, std::bool_constant<false>> ||
      std::is_same_v<T, std::bool_constant<true>>;
};

#define Aligned(type, alignment)            \
  struct alignas(alignment) Aligned##type { \
    type d;                                 \
  }

#define CachelineAligned(type) Aligned(type, nu::kCacheLineBytes)

}  // namespace nu

#include "nu/impl/commons.ipp"

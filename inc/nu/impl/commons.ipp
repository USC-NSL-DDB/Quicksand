extern "C" {
#include <asm/ops.h>
}

#include <limits>

namespace nu {

inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("bsr %q1, %q0" : "=r"(ret) : "m"(a));
  return ret;
}

inline constexpr ProcletHeader *to_proclet_header(ProcletID id) {
  return reinterpret_cast<ProcletHeader *>(id);
}

inline constexpr void *to_proclet_base(ProcletID id) {
  return reinterpret_cast<void *>(id);
}

inline constexpr ProcletID to_proclet_id(void *proclet_base) {
  return reinterpret_cast<ProcletID>(proclet_base);
}

inline constexpr SlabId_t to_slab_id(uint64_t proclet_base_addr) {
  return (proclet_base_addr - kMinProcletHeapVAddr) / kProcletHeapSize + 2;
}

inline constexpr SlabId_t to_slab_id(void *proclet_base) {
  return to_slab_id(reinterpret_cast<uint64_t>(proclet_base));
}

inline constexpr SlabId_t get_max_slab_id() {
  return to_slab_id(kMaxProcletHeapVAddr - kProcletHeapSize);
}

inline __attribute__((always_inline)) void *switch_stack(void *new_rsp) {
  assert(reinterpret_cast<uintptr_t>(new_rsp) % kStackAlignment == 0);
  void *old_rsp;
  asm volatile(
      "movq %%rsp, %0\n\t"
      "movq %1, %%rsp"
      : "=&r"(old_rsp)
      : "r"(new_rsp)
      :);
  return old_rsp;
}

inline VAddrRange get_proclet_stack_range(thread_t *thread) {
  VAddrRange range;
  auto rsp = thread_get_rsp(thread);
  range.start = rsp - kStackRedZoneSize;
  range.end = ((rsp + kStackSize) & (~(kStackSize - 1)));
  return range;
}

inline bool is_in_proclet_heap(void *ptr, void *proclet_base) {
  return (reinterpret_cast<uint64_t>(ptr) & (~(kProcletHeapSize - 1))) ==
         reinterpret_cast<uint64_t>(proclet_base);
}

template <typename T>
std::span<std::byte> to_span(T &t) {
  return std::span<std::byte>(reinterpret_cast<std::byte *>(&t), sizeof(t));
}

template <typename T>
T &from_span(std::span<std::byte> span) {
  return *reinterpret_cast<T *>(span.data());
}

template <typename T>
const T &from_span(std::span<const std::byte> span) {
  return *reinterpret_cast<const T *>(span.data());
}

inline void unblock_and_relax() {
  pause_local_migrating_threads();
  prioritize_local_rcu_readers();
  cpu_relax();
}

}  // namespace nu

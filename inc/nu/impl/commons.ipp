#include <limits>

namespace nu {

inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("bsr %q1, %q0" : "=r"(ret) : "m"(a));
  return ret;
}

inline HeapHeader *to_heap_header(RemObjID id) {
  return reinterpret_cast<HeapHeader *>(id);
}

inline void *to_heap_base(RemObjID id) { return reinterpret_cast<void *>(id); }

inline RemObjID to_obj_id(void *heap_base) {
  return reinterpret_cast<RemObjID>(heap_base);
}

inline __attribute__((always_inline)) void *switch_stack(void *new_rsp) {
  assert(reinterpret_cast<uintptr_t>(new_rsp) % kStackAlignment == 0);
  void *old_rsp;
  asm volatile("movq %%rsp, %0\n\t"
               "movq %1, %%rsp"
               : "=&r"(old_rsp)
               : "r"(new_rsp)
               :);
  return old_rsp;
}

inline VAddrRange get_obj_stack_range(thread_t *thread) {
  VAddrRange range;
  auto rsp = thread_get_rsp(thread);
  range.start = rsp - kStackRedZoneSize;
  range.end = ((rsp + kStackSize) & (~(kStackSize - 1)));
  return range;  
}

inline constexpr uint64_t __to_u16(uint64_t heap_base_addr) {
  return heap_base_addr / kHeapSize;
}

inline uint16_t to_u16(void *heap_base) {
  constexpr auto kMaxHeapBaseAddr = kMaxHeapVAddr - kHeapSize;
  constexpr auto kMinHeapBaseAddr = kMinHeapVAddr;
  static_assert(__to_u16(kMaxHeapBaseAddr) <=
                std::numeric_limits<uint16_t>::max());
  static_assert(__to_u16(kMinHeapBaseAddr) > kRuntimeSlabId);
  return __to_u16(reinterpret_cast<uint64_t>(heap_base));
}

inline bool is_in_heap(void *ptr, void *heap_base) {
  return (reinterpret_cast<uint64_t>(ptr) & (~(kHeapSize - 1))) ==
         reinterpret_cast<uint64_t>(heap_base);
}

template <typename T> std::span<std::byte> to_span(T &t) {
  return std::span<std::byte>(reinterpret_cast<std::byte *>(&t), sizeof(t));
}

template <typename T> T &from_span(std::span<std::byte> span) {
  return *reinterpret_cast<T *>(span.data());
}

template <typename T> const T &from_span(std::span<const std::byte> span) {
  return *reinterpret_cast<const T *>(span.data());
}

} // namespace nu

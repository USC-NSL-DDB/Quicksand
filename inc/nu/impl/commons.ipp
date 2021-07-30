#include <limits>

namespace nu {

inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("bsr %q1, %q0" : "=r"(ret) : "m"(a));
  return ret;
}

inline void *to_heap_base(RemObjID id) { return reinterpret_cast<void *>(id); }

inline RemObjID to_obj_id(void *heap_base) {
  return reinterpret_cast<RemObjID>(heap_base);
}

inline __attribute__((always_inline)) void *switch_to_obj_stack(void *stack) {
  void *old_rsp;
  asm volatile("movq %%rsp, %0\n\t"
               "movq %1, %%rsp"
               : "=&r"(old_rsp)
               : "r"(stack)
               :);
  return old_rsp;
}

inline __attribute__((always_inline)) void
switch_to_runtime_stack(void *old_rsp) {
  asm volatile("movq %0, %%rsp" : : "r"(old_rsp) :);
}

inline VAddrRange get_obj_stack_range(thread_t *thread) {
  VAddrRange range;
  auto rsp = thread_get_rsp(thread);
  range.start = rsp - kStackRedZoneSize;
  range.end = ((rsp + kStackSize) & (~(kStackSize - 1)));
  return range;  
}

inline constexpr uint64_t __to_slab_id(uint64_t heap_base_addr) {
  return heap_base_addr / kHeapSize;
}

inline SlabId_t to_slab_id(void *heap_base) {
  constexpr auto kMaxHeapBaseAddr = kMaxHeapVAddr - kHeapSize;
  constexpr auto kMinHeapBaseAddr = kMinHeapVAddr;
  static_assert(__to_slab_id(kMaxHeapBaseAddr) <=
                std::numeric_limits<SlabId_t>::max());
  static_assert(__to_slab_id(kMinHeapBaseAddr) > kRuntimeSlabId);
  return __to_slab_id(reinterpret_cast<uint64_t>(heap_base));
}

} // namespace nu

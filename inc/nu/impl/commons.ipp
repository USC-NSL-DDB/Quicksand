#include <limits>

namespace nu {

inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("bsr %q1, %q0" : "=r"(ret) : "m"(a));
  return ret;
}

inline constexpr HeapHeader *to_heap_header(RemObjID id) {
  return reinterpret_cast<HeapHeader *>(id);
}

inline constexpr void *to_heap_base(RemObjID id) {
  return reinterpret_cast<void *>(id);
}

inline constexpr RemObjID to_obj_id(void *heap_base) {
  return reinterpret_cast<RemObjID>(heap_base);
}

inline constexpr SlabId_t to_slab_id(uint64_t heap_base_addr) {
  return (heap_base_addr - kMinHeapVAddr) / kHeapSize + 2;
}

inline constexpr SlabId_t to_slab_id(void *heap_base) {
  return to_slab_id(reinterpret_cast<uint64_t>(heap_base));
}

inline constexpr SlabId_t get_max_slab_id() {
  return to_slab_id(kMaxHeapVAddr - kHeapSize);
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

inline bool is_in_heap(void *ptr, void *heap_base) {
  return (reinterpret_cast<uint64_t>(ptr) & (~(kHeapSize - 1))) ==
         reinterpret_cast<uint64_t>(heap_base);
}

inline bool is_in_stack(void *ptr, VAddrRange stack) {
  auto ptr_addr = reinterpret_cast<uint64_t>(ptr);
  return ptr_addr >= stack.start && ptr_addr < stack.end;
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

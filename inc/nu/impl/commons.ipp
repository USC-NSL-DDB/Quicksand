extern "C" {
#include <runtime/thread.h>
}

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

} // namespace nu

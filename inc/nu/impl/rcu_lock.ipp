#include <thread.h>
#include <sync.h>

namespace nu {

inline uint32_t RCULock::reader_lock() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  return reader_lock(g);
}

inline uint32_t RCULock::reader_lock(const rt::PreemptGuard &g) {
  auto flag = load_acquire(&flag_);
  auto nesting_cnt = thread_hold_rcu(this, flag);
  if (nesting_cnt == 1) {
    auto &cnt_ref = aligned_cnts_[flag][read_cpu()].cnt;
    AlignedCnt::Cnt cnt_copy;
    cnt_copy.raw = cnt_ref.raw;
    cnt_copy.val++;
    cnt_copy.ver++;
    cnt_ref.raw = cnt_copy.raw;
  }
  barrier();
  assert(nesting_cnt >= 1);
  return nesting_cnt;
}

inline void RCULock::reader_unlock() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  reader_unlock(g);
}

inline void RCULock::reader_unlock(const rt::PreemptGuard &g) {
  barrier();
  bool flag;
  auto nesting_cnt = thread_unhold_rcu(this, &flag);
  assert(nesting_cnt >= 0);
  if (!nesting_cnt) {
    auto &cnt_ref = aligned_cnts_[flag][read_cpu()].cnt;
    AlignedCnt::Cnt cnt_copy;
    cnt_copy.raw = cnt_ref.raw;
    cnt_copy.val--;
    cnt_ref.raw = cnt_copy.raw;
  }
}

}  // namespace nu

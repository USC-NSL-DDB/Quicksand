#include <sync.h>

#include <cstring>

namespace nu {

inline Counter::Counter() { reset(); }

inline void Counter::inc() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);
  cnts_[p.get_cpu()].c++;
}

inline void Counter::inc_unsafe() { cnts_[read_cpu()].c++; }

inline void Counter::dec() {
  rt::Preempt p;
  rt::PreemptGuard g(&p);
  cnts_[p.get_cpu()].c--;
}

inline void Counter::dec_unsafe() { cnts_[read_cpu()].c--; }

inline int64_t Counter::get() {
  int64_t sum = 0;
  for (auto &cnt : cnts_) {
    sum += cnt.c;
  }
  return sum;
}

inline void Counter::reset() { memset(cnts_, 0, sizeof(cnts_)); }

}  // namespace nu

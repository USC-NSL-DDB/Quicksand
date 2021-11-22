extern "C" {
#include <base/assert.h>
#include <runtime/preempt.h>
}

namespace nu {

inline void ThreadSet::put(thread_t *th) {
  auto core_id = read_cpu();
  put(th, core_id);
  set_thread_set_idx(th, core_id);
}

inline void ThreadSet::put(thread_t *th, int8_t core_id) {
  auto &data = data_[core_id];
  data.spin.lock();
  data.set.emplace(th);
  data.spin.unlock();
}

inline void ThreadSet::remove(thread_t *th) {
  auto core_id = get_thread_set_idx(th);
  auto &data = data_[core_id];
  data.spin.lock();
  BUG_ON(!data.set.erase(th));
  data.spin.unlock();
}

} // namespace nu

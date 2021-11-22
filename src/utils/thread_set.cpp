#include "nu/utils/thread_set.hpp"

namespace nu {

std::vector<thread_t *> ThreadSet::all_threads() {
  std::vector<thread_t *> threads;

  for (uint32_t i = 0; i < kNumCores; i++) {
    auto &data = data_[i];
    data.spin.lock();
    threads.insert(threads.end(), data.set.begin(), data.set.end());
    data.spin.unlock();
  }
  return threads;
}
} // namespace nu

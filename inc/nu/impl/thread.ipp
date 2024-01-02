#include "nu/utils/caladan.hpp"

namespace nu {

extern void trampoline_in_proclet_env(void *arg);
extern void trampoline_in_runtime_env(void *arg);

inline Thread::Thread() : join_data_(nullptr) {}

inline Thread::~Thread() { BUG_ON(join_data_); }

inline Thread::Thread(Thread &&t) { *this = std::move(t); }

inline Thread &Thread::operator=(Thread &&t) {
  join_data_ = t.join_data_;
  t.join_data_ = nullptr;
  return *this;
}

inline bool Thread::joinable() { return join_data_; }

inline uint64_t Thread::get_id() { return id_; }

template <typename T, typename F>
void parallel_for(T begin_idx, T end_idx, F &&f, bool head) {
  std::vector<nu::Thread> ths;
  auto num_ths = nu::Caladan::get_max_cores();
  ths.reserve(num_ths);
  auto chunk_size =
      (static_cast<int64_t>(end_idx - begin_idx) - 1) / num_ths + 1;
  for (uint32_t i = 0; i < num_ths; i++) {
    ths.emplace_back(
        [&, tid = i] {
          T chunk_begin = begin_idx + chunk_size * tid;
          T chunk_end = chunk_begin + chunk_size;
          chunk_end = std::min(chunk_end, end_idx);
          for (auto idx = chunk_begin; idx < chunk_end; ++idx) {
            f(idx);
          }
        },
        head);
  }
  for (auto &th : ths) {
    th.join();
  }
}

}  // namespace nu

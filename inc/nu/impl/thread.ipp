#include <type_traits>

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

template <typename T, typename F>
auto parallel_for_range(T begin_idx, T end_idx, F &&f, bool head) {
  using FRetT = std::invoke_result_t<F, T, T>;
  constexpr bool FVoid = std::is_void_v<FRetT>;

  std::vector<nu::Thread> ths;
  std::vector<std::conditional_t<FVoid, ErasedType, FRetT>> all_rets;

  auto num_ths = nu::Caladan::get_max_cores();
  ths.reserve(num_ths);
  all_rets.resize(num_ths);

  auto chunk_size =
      (static_cast<int64_t>(end_idx - begin_idx) - 1) / num_ths + 1;
  for (uint32_t i = 0; i < num_ths; i++) {
    ths.emplace_back(
        [&, tid = i] {
          auto &rets = all_rets[tid];
          T chunk_begin = begin_idx + chunk_size * tid;
          T chunk_end = chunk_begin + chunk_size;
          chunk_end = std::min(chunk_end, end_idx);
          if constexpr (!FVoid) {
            rets = f(chunk_begin, chunk_end);
          }
        },
        head);
  }

  for (auto &th : ths) {
    th.join();
  }

  if constexpr (!FVoid) {
    return all_rets;
  }
}

}  // namespace nu

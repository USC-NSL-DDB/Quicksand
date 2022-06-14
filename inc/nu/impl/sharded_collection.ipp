#include <cstdint>

#include "nu/sharded_collection.hpp"

namespace nu {

template <typename T>
ShardedCollection<T>::ShardedCollection(uint32_t shard_size)
    : shard_size_(shard_size), shard_(make_proclet<std::vector<T>>()) {}

template <typename T>
template <typename U>
void ShardedCollection<T>::emplace_back(U &&u) {
  shard_.run(
      +[](std::vector<T> &v, U u) { v.emplace_back(u); }, std::forward<U>(u));
}

template <typename T>
void ShardedCollection<T>::emplace_back_batch(const std::vector<T> &v) {
  shard_.run(
      +[](std::vector<T> &v, std::vector<T> new_v) {
        v.insert(v.end(), new_v.begin(), new_v.end());
      },
      v);
}

template <typename T>
template <typename... S0s, typename... S1s>
void ShardedCollection<T>::for_all(void (*fn)(T &, S0s...), S1s &&... states) {
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  shard_.run(
      +[](std::vector<T> &v, uintptr_t raw_fn, S1s... states) {
        auto *fn = reinterpret_cast<Fn>(raw_fn);
        for (auto &t : v) {
          fn(t, std::move(states)...);
        }
      },
      raw_fn, std::forward<S1s>(states)...);
}

template <typename T>
std::vector<T> ShardedCollection<T>::collect() {
  return shard_.run(+[](std::vector<T> &v) { return v; });
}

}  // namespace nu

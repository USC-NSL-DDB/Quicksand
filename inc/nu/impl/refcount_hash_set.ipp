#include <functional>
#include <utility>

extern "C" {
#include <base/assert.h>
#include <runtime/preempt.h>
}

namespace nu {

template <typename K, typename Allocator>
template <typename K1>
void RefcountHashSet<K, Allocator>::put(K1 &&k) {
  int cpu = get_cpu();
  auto &map = ref_counts_[cpu];
  auto iter = map.try_emplace(k, 0).first;
  ++iter->second;
  put_cpu();
}

template <typename K, typename Allocator>
template <typename K1>
void RefcountHashSet<K, Allocator>::remove(K1 &&k) {
  int cpu = get_cpu();
  auto &map = ref_counts_[cpu];
  auto iter = map.try_emplace(k, 0).first;
  --iter->second;
  put_cpu();
}

template <typename K, typename Allocator>
std::vector<K, Allocator> RefcountHashSet<K, Allocator>::all_keys() {
  std::unordered_map<K, V> sum_map;
  for (size_t i = 0; i < kNumCores; i++) {
    auto iter = ref_counts_[i].begin();
    while (iter != ref_counts_[i].end()) {
      const auto &[k, cnt] = *iter;
      if (cnt) {
        sum_map[k] += cnt;
        iter++;
      } else {
        iter = ref_counts_[i].erase(iter);
      }
    }
  }

  std::vector<K, Allocator> keys;
  for (auto &[k, cnt] : sum_map) {
    BUG_ON(cnt != 0 && cnt != 1);
    if (cnt) {
      keys.push_back(k);
    }
  }

  return keys;
}

} // namespace nu

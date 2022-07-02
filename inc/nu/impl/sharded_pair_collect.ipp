#include <algorithm>
#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>

#include <sync.h>

#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

namespace nu {

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection() {}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    uint32_t max_shard_bytes, uint32_t max_per_core_cache_bytes,
    std::optional<K> initial_l_key, std::optional<K> initial_r_key)
    : mapping_(make_proclet<ShardingMapping>()),
      max_shard_size_(max_shard_bytes / sizeof(PairType)),
      max_per_core_cache_size_(max_per_core_cache_bytes / sizeof(PairType)) {
  auto initial_shard =
      make_proclet<Shard>(mapping_.get_weak(), max_shard_size_, initial_l_key,
                          initial_r_key, ShardDataType());
  auto weak_shard = initial_shard.get_weak();
  add_cache_to_all_cores(std::nullopt, weak_shard);
  mapping_.run(&ShardingMapping::update_mapping, initial_l_key,
               std::move(initial_shard));
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
    uint32_t max_per_core_cache_bytes)
    : ShardedPairCollection(max_shard_bytes, max_per_core_cache_bytes,
                            std::optional<K>(), estimated_min_key) {
  auto num_shards = (num - 1) / max_shard_size_ + 1;
  std::vector<Future<Proclet<Shard>>> shard_futures;

  auto k = estimated_min_key;
  for (uint32_t i = 0; i < num_shards; i++) {
    auto prev_k = k;
    key_inc_fn(k, max_shard_size_);
    shard_futures.emplace_back(make_proclet_async<Shard>(
        mapping_.get_weak(), max_shard_size_, prev_k,
        (i != num_shards - 1) ? k : std::optional<K>(), ShardDataType()));
  }

  k = estimated_min_key;
  for (auto &shard_future : shard_futures) {
    auto &shard = shard_future.get();
    auto weak_shard = shard.get_weak();
    auto update_future = mapping_.run_async(&ShardingMapping::update_mapping, k,
                                            std::move(shard));
    add_cache_to_all_cores(k, weak_shard);
    key_inc_fn(k, max_shard_size_);
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    const ShardedPairCollection &o) {
  *this = o;
}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    const ShardedPairCollection &o) {
  mapping_ = o.mapping_;
  max_shard_size_ = o.max_shard_size_;
  max_per_core_cache_size_ = o.max_per_core_cache_size_;
  node_proxy_shards_ = o.node_proxy_shards_;
  for (uint32_t i = 0; i < kNumCores; i++) {
    for (auto &[k, c] : o.per_cores_[i].key_to_cache) {
      add_cache_to_core(i, k, c.shard);
    }
  }
  return *this;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardedPairCollection(
    ShardedPairCollection &&o) noexcept {
  *this = std::move(o);
}

template <typename K, typename V>
ShardedPairCollection<K, V> &ShardedPairCollection<K, V>::operator=(
    ShardedPairCollection &&o) noexcept {
  mapping_ = std::move(o.mapping_);
  max_shard_size_ = o.max_shard_size_;
  max_per_core_cache_size_ = o.max_per_core_cache_size_;
  node_proxy_shards_ = std::move(o.node_proxy_shards_);
  for (uint32_t i = 0; i < kNumCores; i++) {
    per_cores_[i].key_to_cache = std::move(o.per_cores_[i].key_to_cache);
    per_cores_[i].ip_to_caches = std::move(o.per_cores_[i].ip_to_caches);
  }
  return *this;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::~ShardedPairCollection() {
  flush();
}

template <typename K, typename V>
std::vector<typename ShardedPairCollection<K, V>::PushDataReq>
ShardedPairCollection<K, V>::gen_push_data_reqs(uint32_t core_id, NodeIP ip) {
  std::vector<PushDataReq> reqs;

  auto &[cache_size, cache_iters] = per_cores_[core_id].ip_to_caches[ip];
  for (auto cache_iter : cache_iters) {
    auto &[l_key, cache] = *cache_iter;
    auto &shard = cache.shard;
    auto &r_key = (--cache_iter)->first;
    if (!cache.data.empty()) {
      reqs.emplace_back(l_key, r_key, shard, std::move(cache.data));
      cache.data.clear();
    }
  }
  cache_size = 0;
  return reqs;
}

template <typename K, typename V>
template <typename K1, typename V1>
void ShardedPairCollection<K, V>::emplace(K1 &&k, V1 &&v) {
  emplace({std::forward<K1>(k), std::forward<V1>(v)});
}

template <typename K, typename V>
bool ShardedPairCollection<K, V>::__emplace(PairType &&p) {
  assert_preempt_disabled();

  auto core_id = read_cpu();
  auto &per_core = per_cores_[core_id];
  auto &key_to_cache = per_core.key_to_cache;
  auto iter = key_to_cache.lower_bound(p.first);
  assert(iter != key_to_cache.end());
  auto &cache = iter->second;

  cache.data.emplace_back(std::move(p));
  auto &cache_size = ++(*cache.per_ip_cache_size);

  if (unlikely(cache_size >= max_per_core_cache_size_)) {
    auto proclet_id = iter->second.shard.get_id();
    put_cpu();
    auto ip = Runtime::get_ip_by_proclet_id(proclet_id);
    get_cpu();
    if (likely(read_cpu() == core_id)) {
      auto reqs = gen_push_data_reqs(core_id, ip);
      put_cpu();
      if (!reqs.empty()) {
        push_data(ip, std::move(reqs));
      }
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::emplace(PairType &&p) {
  get_cpu();

  if (!__emplace(std::move(p))) {
    put_cpu();
  }
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::add_cache_to_core(int core_id,
                                                    std::optional<K> k,
                                                    WeakProclet<Shard> shard) {
  auto [iter, _] =
      per_cores_[core_id].key_to_cache.try_emplace(std::move(k), Cache(shard));
  auto ip = Runtime::get_ip_by_proclet_id(shard.get_id());
  bind_cache_to_core(core_id, ip, iter);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::add_cache_to_cur_core(
    std::optional<K> k, WeakProclet<Shard> shard) {
  auto ip = Runtime::get_ip_by_proclet_id(shard.get_id());

  rt::Preempt p;
  rt::PreemptGuard g(&p);
  auto core_id = p.get_cpu();
  auto [iter, _] =
      per_cores_[core_id].key_to_cache.try_emplace(std::move(k), Cache(shard));
  bind_cache_to_core(core_id, ip, iter);
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::add_cache_to_all_cores(
    std::optional<K> k, WeakProclet<Shard> shard) {
  for (uint32_t i = 0; i < kNumCores; i++) {
    add_cache_to_core(i, k, shard);
  }
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::bind_cache_to_core(
    int core_id, NodeIP ip, const KeyToCacheMappingType::iterator &cache_iter) {
  auto &ip_to_caches = per_cores_[core_id].ip_to_caches;
  auto iter = ip_to_caches.find(ip);
  if (unlikely(iter == ip_to_caches.end())) {
    auto p = ip_to_caches.try_emplace(
        ip, 0, std::list<typename KeyToCacheMappingType::iterator>());
    iter = p.first;
  }
  iter->second.second.emplace_back(cache_iter);
  cache_iter->second.per_ip_cache_size = &iter->second.first;
}

template <typename K, typename V>
WeakProclet<ErasedType> ShardedPairCollection<K, V>::get_node_proxy_shard(
    NodeIP ip) {
  rw_lock_.reader_lock();
  auto iter = node_proxy_shards_.find(ip);
  if (unlikely(iter == node_proxy_shards_.end())) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (likely(!node_proxy_shards_.contains(ip))) {
      node_proxy_shards_.try_emplace(ip,
                                     make_proclet_pinned_at<ErasedType>(ip));
    }
    iter = node_proxy_shards_.find(ip);
    rw_lock_.writer_unlock();
  } else {
    rw_lock_.reader_unlock();
  }
  return iter->second.get_weak();
}

template <typename K, typename V>
bool ShardedPairCollection<K, V>::push_data(NodeIP ip,
                                            std::vector<PushDataReq> reqs) {
  auto node_proxy_shard = get_node_proxy_shard(ip);

  auto rejected_req_indices = node_proxy_shard.run(
      +[](ErasedType &_, std::vector<PushDataReq> reqs) {
        std::vector<uint32_t> rejected_req_indices;

        for (uint64_t i = 0; i < reqs.size(); i++) {
          auto &req = reqs[i];
          auto &[l_key, r_key, shard, data] = req;
          bool success =
              shard.run(&Shard::try_emplace_back, l_key, r_key, data);
          if (unlikely(!success)) {
            rejected_req_indices.push_back(i);
          }
        }
        return rejected_req_indices;
      },
      reqs);

  if (!rejected_req_indices.empty()) {
    for (auto idx : rejected_req_indices) {
      auto &[l_key, r_key, shard, rejected_data] = reqs[idx];
      auto new_mappings =
          mapping_.run(&ShardingMapping::get_shards_in_range, l_key, r_key);

      for (auto &[k, s] : new_mappings) {
        add_cache_to_cur_core(std::move(k), s);
      }

      rt::Preempt pt;
      rt::PreemptGuard g(&pt);
      for (auto &p : rejected_data) {
        if (__emplace(std::move(p))) {
          get_cpu();
        }
      }
    }
    return false;
  }

  return true;
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::flush() {
again:
  std::vector<Thread> ths;
  std::vector<std::pair<NodeIP, std::vector<PushDataReq>>>
      all_ip_reqs[kNumCores];

  for (uint32_t i = 0; i < kNumCores; i++) {
    ths.emplace_back([this, tid = i, ip_reqs_ptr = &all_ip_reqs[i]] {
      for (auto &[ip, _] : per_cores_[tid].ip_to_caches) {
        auto reqs = gen_push_data_reqs(tid, ip);
        if (!reqs.empty()) {
          ip_reqs_ptr->emplace_back(ip, std::move(reqs));
        }
      }
    });
  }
  for (auto &th : ths) {
    th.join();
  }
  ths.clear();

  std::vector<Future<bool>> futures;
  for (uint32_t i = 0; i < kNumCores; i++) {
    futures.emplace_back(nu::async([this, ip_reqs_ptr = &all_ip_reqs[i]] {
      bool done = true;
      for (auto &[ip, reqs] : *ip_reqs_ptr) {
        done &= push_data(ip, std::move(reqs));
      }
      return done;
    }));
  }

  bool done = true;
  for (auto &future : futures) {
    done &= future.get();
  }

  if (unlikely(!done)) {
    futures.clear();
    goto again;
  }
}

template <typename K, typename V>
template <typename... S0s, typename... S1s>
void ShardedPairCollection<K, V>::for_all(void (*fn)(std::pair<const K, V> &p,
                                                     S0s...),
                                          S1s &&... states) {
  flush();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<void>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto pair = shard.get_data_ptr();
          for (auto &t : *pair.second) {
            fn(reinterpret_cast<std::pair<const K, V> &>(t),
               std::move(states)...);
          }
        },
        raw_fn, states...));
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::collect() {
  flush();

  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<ShardDataType>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(&Shard::get_data));
  }
  ShardDataType all;
  for (auto &future : futures) {
    auto &vec = future.get();
    all.insert(all.end(), vec.begin(), vec.end());
  }
  return all;
}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::save(Archive &ar) const {
  ar(mapping_, max_shard_size_, max_per_core_cache_size_, node_proxy_shards_);
  for (const auto &per_core : per_cores_) {
    ar(per_core.key_to_cache);
  }
}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::load(Archive &ar) {
  ar(mapping_, max_shard_size_, max_per_core_cache_size_, node_proxy_shards_);
  for (uint32_t i = 0; i < kNumCores; i++) {
    auto &key_to_cache = per_cores_[i].key_to_cache;
    ar(key_to_cache);
    for (auto iter = key_to_cache.begin(); iter != key_to_cache.end(); iter++) {
      auto ip = Runtime::get_ip_by_proclet_id(iter->second.shard.get_id());
      bind_cache_to_core(i, ip, iter);
    }
  }
}

template <typename K, typename V>
ShardedPairCollection<K, V>::Shard::Shard(WeakProclet<ShardingMapping> mapping,
                                          uint32_t max_shard_size,
                                          std::optional<K> l_key,
                                          std::optional<K> r_key,
                                          ShardDataType data)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key) {
  data_.reserve(max_shard_size_);
  data_ = data;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::Shard::Shard(WeakProclet<ShardingMapping> mapping,
                                          uint32_t max_shard_size,
                                          std::optional<K> l_key,
                                          std::optional<K> r_key,
                                          SpanToVectorWrapper<PairType> data)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      data_(std::move(data.unwrap())) {}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardDataType
ShardedPairCollection<K, V>::Shard::get_data() {
  ScopedLock<Mutex> guard(&mutex_);
  return data_;
}

template <typename K, typename V>
std::pair<ScopedLock<Mutex>,
          typename ShardedPairCollection<K, V>::ShardDataType *>
ShardedPairCollection<K, V>::Shard::get_data_ptr() {
  return std::make_pair(&mutex_, &data_);
}

template <typename K, typename V>
bool ShardedPairCollection<K, V>::Shard::try_emplace_back(
    std::optional<K> l_key, std::optional<K> r_key, ShardDataType data) {
  ScopedLock<Mutex> guard(&mutex_);

  bool bad_range = (l_key < l_key_) || (r_key_ && ((r_key > r_key_) || !r_key));
  if (unlikely(bad_range)) {
    return false;
  }

  data_.insert(data_.end(), std::make_move_iterator(data.begin()),
               std::make_move_iterator(data.end()));

  if (unlikely(data_.size() > max_shard_size_)) {
    auto mid = data_.begin() + data_.size() / 2;
    std::nth_element(data_.begin(), mid, data_.end());
    auto mid_k = data_[data_.size() / 2].first;
    SpanToVectorWrapper post_split_data(std::span(mid, data_.end()));
    auto new_shard = make_proclet<Shard>(mapping_, max_shard_size_, mid_k,
                                         r_key_, post_split_data);
    data_.erase(mid, data_.end());
    r_key_ = mid_k;
    mapping_.run(&ShardingMapping::update_mapping, mid_k, std::move(new_shard));
  }
  return true;
}

template <typename K, typename V>
ShardedPairCollection<K, V>::ShardingMapping::ShardingMapping() {}

template <typename K, typename V>
std::vector<std::pair<std::optional<K>,
                      WeakProclet<typename ShardedPairCollection<K, V>::Shard>>>
ShardedPairCollection<K, V>::ShardingMapping::get_shards_in_range(
    std::optional<K> l_key, std::optional<K> r_key) {
  std::vector<std::pair<std::optional<K>, WeakProclet<Shard>>> shards;

  rw_lock_.reader_lock();
  typename decltype(mapping_)::iterator iter =
      r_key ? mapping_.upper_bound(r_key) : mapping_.begin();
  while (iter != mapping_.end() && iter->first >= l_key) {
    shards.emplace_back(iter->first, iter->second.get_weak());
    iter++;
  }
  rw_lock_.reader_unlock();

  return shards;
}

template <typename K, typename V>
std::vector<WeakProclet<typename ShardedPairCollection<K, V>::Shard>>
ShardedPairCollection<K, V>::ShardingMapping::get_all_shards() {
  std::vector<WeakProclet<Shard>> shards;

  rw_lock_.reader_lock();
  for (auto &[_, shard] : mapping_) {
    shards.emplace_back(shard.get_weak());
  }
  rw_lock_.reader_unlock();

  return shards;
}

template <typename K, typename V>
void ShardedPairCollection<K, V>::ShardingMapping::update_mapping(
    std::optional<K> k, Proclet<Shard> shard) {
  rw_lock_.writer_lock();
  auto ret = mapping_.try_emplace(k, std::move(shard));
  rw_lock_.writer_unlock();
  BUG_ON(!ret.second);
}

template <typename K, typename V>
ShardedPairCollection<K, V>::Cache::Cache() {}

template <typename K, typename V>
ShardedPairCollection<K, V>::Cache::Cache(WeakProclet<Shard> s) : shard(s) {}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::Cache::save(Archive &ar) const {
  ar(shard);
}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::Cache::load(Archive &ar) {
  ar(shard);
}

template <typename K, typename V>
template <class Archive>
void ShardedPairCollection<K, V>::PushDataReq::serialize(Archive &ar) {
  ar(l_key, r_key, shard, data);
}

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint32_t max_shard_bytes, uint32_t max_per_core_cache_bytes) {
  return ShardedPairCollection<K, V>(max_shard_bytes, max_per_core_cache_bytes,
                                     std::optional<K>(), std::optional<K>());
}

template <typename K, typename V>
ShardedPairCollection<K, V> make_sharded_pair_collection(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
    uint32_t max_per_core_cache_bytes) {
  return ShardedPairCollection<K, V>(num, std::move(estimated_min_key),
                                     std::move(key_inc_fn), max_shard_bytes,
                                     max_per_core_cache_bytes);
}

}  // namespace nu

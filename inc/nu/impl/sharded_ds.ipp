#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>

// TODO: support no-cache mode.

namespace nu {

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_size,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      Container container)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key) {
  container_.reserve(max_shard_size_);
  container_ = container;
}

template <class Container>
Container GeneralShard<Container>::get_container() {
  ScopedLock<Mutex> guard(&mutex_);
  return container_;
}

template <class Container>
std::pair<ScopedLock<Mutex>, Container *>
GeneralShard<Container>::get_container_ptr() {
  return std::make_pair(&mutex_, &container_);
}

template <class Container>
bool GeneralShard<Container>::try_emplace_batch(std::optional<Key> l_key,
                                                std::optional<Key> r_key,
                                                Container container) {
  ScopedLock<Mutex> guard(&mutex_);

  bool bad_range = (l_key < l_key_) || (r_key_ && ((r_key > r_key_) || !r_key));
  if (unlikely(bad_range)) {
    return false;
  }

  if (unlikely(container_.size() + container.size() > max_shard_size_)) {
    Key mid_k;
    Container post_split_container;
    container_.split(&mid_k, &post_split_container);
    auto new_shard = make_proclet<GeneralShard>(
        mapping_, max_shard_size_, mid_k, r_key_, post_split_container);
    r_key_ = mid_k;
    mapping_.run(&ShardingMapping::update_mapping, mid_k, std::move(new_shard));
    return false;
  }

  container_.emplace_batch(container);

  return true;
}

template <class Shard>
std::vector<std::pair<std::optional<typename Shard::Key>, WeakProclet<Shard>>>
GeneralShardingMapping<Shard>::get_shards_in_range(std::optional<Key> l_key,
                                                   std::optional<Key> r_key) {
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>> shards;

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

template <class Shard>
std::vector<WeakProclet<Shard>>
GeneralShardingMapping<Shard>::get_all_shards() {
  std::vector<WeakProclet<Shard>> shards;

  rw_lock_.reader_lock();
  for (auto &[_, shard] : mapping_) {
    shards.emplace_back(shard.get_weak());
  }
  rw_lock_.reader_unlock();

  return shards;
}

template <class Shard>
void GeneralShardingMapping<Shard>::update_mapping(std::optional<Key> k,
                                                   Proclet<Shard> shard) {
  rw_lock_.writer_lock();
  auto ret = mapping_.try_emplace(k, std::move(shard));
  rw_lock_.writer_unlock();
  BUG_ON(!ret.second);
}

template <class Container>
ShardedDataStructure<Container>::ShardedDataStructure() {}

template <class Container>
ShardedDataStructure<Container>::ShardedDataStructure(
    std::optional<Key> initial_l_key, std::optional<Key> initial_r_key,
    uint32_t max_shard_bytes, uint32_t max_cache_bytes)
    : mapping_(make_proclet<ShardingMapping>()),
      max_shard_size_(max_shard_bytes / sizeof(Pair)),
      max_cache_size_(max_cache_bytes / sizeof(Pair)) {
  auto initial_shard =
      make_proclet<Shard>(mapping_.get_weak(), max_shard_size_, initial_l_key,
                          initial_r_key, Container());
  auto weak_shard = initial_shard.get_weak();
  add_cache(std::nullopt, weak_shard);
  mapping_.run(&ShardingMapping::update_mapping, initial_l_key,
               std::move(initial_shard));
}

template <class Container>
ShardedDataStructure<Container>::ShardedDataStructure(
    uint64_t num, Key estimated_min_key,
    std::function<void(Key &, uint64_t)> key_inc_fn, uint32_t max_shard_bytes,
    uint32_t max_cache_bytes)
    : ShardedDataStructure(std::optional<Key>(), estimated_min_key,
                           max_shard_bytes, max_cache_bytes) {
  auto num_shards = (num - 1) / max_shard_size_ + 1;
  std::vector<Future<Proclet<Shard>>> shard_futures;

  auto k = estimated_min_key;
  for (uint32_t i = 0; i < num_shards; i++) {
    auto prev_k = k;
    key_inc_fn(k, max_shard_size_);
    shard_futures.emplace_back(make_proclet_async<Shard>(
        mapping_.get_weak(), max_shard_size_, prev_k,
        (i != num_shards - 1) ? k : std::optional<Key>(), Container()));
  }

  k = estimated_min_key;
  for (auto &shard_future : shard_futures) {
    auto &shard = shard_future.get();
    auto weak_shard = shard.get_weak();
    auto update_future = mapping_.run_async(&ShardingMapping::update_mapping, k,
                                            std::move(shard));
    add_cache(k, weak_shard);
    key_inc_fn(k, max_shard_size_);
  }
}

template <class Container>
ShardedDataStructure<Container>::ShardedDataStructure(
    const ShardedDataStructure &o) {
  *this = o;
}

template <class Container>
ShardedDataStructure<Container> &ShardedDataStructure<Container>::operator=(
    const ShardedDataStructure &o) {
  mapping_ = o.mapping_;
  max_shard_size_ = o.max_shard_size_;
  max_cache_size_ = o.max_cache_size_;
  node_proxy_shards_ = o.node_proxy_shards_;
  for (auto &[k, c] : o.key_to_cache_) {
    add_cache(k, c.shard);
  }
  return *this;
}

template <class Container>
ShardedDataStructure<Container>::ShardedDataStructure(
    ShardedDataStructure &&o) noexcept {
  *this = std::move(o);
}

template <class Container>
ShardedDataStructure<Container> &ShardedDataStructure<Container>::operator=(
    ShardedDataStructure &&o) noexcept {
  mapping_ = std::move(o.mapping_);
  max_shard_size_ = o.max_shard_size_;
  max_cache_size_ = o.max_cache_size_;
  node_proxy_shards_ = std::move(o.node_proxy_shards_);
  key_to_cache_ = std::move(o.key_to_cache_);
  ip_to_caches_ = std::move(o.ip_to_caches_);
  push_future_ = std::move(o.push_future_);
  rejected_push_reqs_ = std::move(o.rejected_push_reqs_);
  return *this;
}

template <class Container>
ShardedDataStructure<Container>::~ShardedDataStructure() {
  flush();
}

template <class Container>
std::vector<typename ShardedDataStructure<Container>::PushDataReq>
ShardedDataStructure<Container>::gen_push_data_reqs(NodeIP ip) {
  std::vector<PushDataReq> reqs;

  auto &[cache_size, cache_iters] = ip_to_caches_[ip];
  for (auto cache_iter : cache_iters) {
    auto &[l_key, cache] = *cache_iter;
    auto &shard = cache.shard;
    auto &r_key = (--cache_iter)->first;
    if (!cache.container.empty()) {
      reqs.emplace_back(l_key, r_key, shard, std::move(cache.container));
      cache.container.clear();
    }
  }
  cache_size = 0;
  return reqs;
}

template <class Container>
template <typename K1, typename V1>
void ShardedDataStructure<Container>::emplace(K1 &&k, V1 &&v) {
  emplace({std::forward<K1>(k), std::forward<V1>(v)});
}

template <class Container>
void ShardedDataStructure<Container>::emplace(Pair &&p) {
  if (unlikely(!rejected_push_reqs_.empty())) {
    spin_.lock();
    auto reqs = std::move(rejected_push_reqs_);
    rejected_push_reqs_.clear();
    spin_.unlock();
    handle_rejected_push_reqs(reqs);
  }

  auto iter = key_to_cache_.lower_bound(p.first);
  assert(iter != key_to_cache_.end());
  auto &cache = iter->second;

  cache.container.emplace(std::move(p.first), std::move(p.second));
  auto &cache_size = ++(*cache.per_ip_cache_size);

  if (unlikely(cache_size >= max_cache_size_)) {
    auto proclet_id = iter->second.shard.get_id();
    auto ip = Runtime::get_ip_by_proclet_id(proclet_id);
    auto reqs = gen_push_data_reqs(ip);
    if (!reqs.empty()) {
      submit_push_data_req(ip, std::move(reqs));
    }
  }
}

template <class Container>
void ShardedDataStructure<Container>::add_cache(std::optional<Key> k,
                                                WeakProclet<Shard> shard) {
  auto ip = Runtime::get_ip_by_proclet_id(shard.get_id());
  auto [iter, _] = key_to_cache_.try_emplace(std::move(k), Cache(shard));
  bind_cache(ip, iter);
}

template <class Container>
void ShardedDataStructure<Container>::bind_cache(
    NodeIP ip, const KeyToCacheMapping::iterator &cache_iter) {
  auto iter = ip_to_caches_.find(ip);
  if (unlikely(iter == ip_to_caches_.end())) {
    auto p = ip_to_caches_.try_emplace(
        ip, 0, std::list<typename KeyToCacheMapping::iterator>());
    iter = p.first;
  }
  iter->second.second.emplace_back(cache_iter);
  cache_iter->second.per_ip_cache_size = &iter->second.first;
}

template <class Container>
WeakProclet<ErasedType> ShardedDataStructure<Container>::get_node_proxy_shard(
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

template <class Container>
void ShardedDataStructure<Container>::submit_push_data_req(
    NodeIP ip, std::vector<PushDataReq> reqs) {
  auto node_proxy_shard = get_node_proxy_shard(ip);
  if (push_future_) {
    auto &rejected_reqs = push_future_.get();
    if (unlikely(!rejected_reqs.empty())) {
      ScopedLock g(&spin_);
      BUG_ON(!rejected_push_reqs_.empty());
      rejected_push_reqs_ = std::move(rejected_reqs);
    }
  }

  push_future_ =
      nu::async([node_proxy_shard, reqs = std::move(reqs)]() mutable {
        auto rejected_req_indices = node_proxy_shard.run(
            +[](ErasedType &_, std::vector<PushDataReq> reqs) {
              std::vector<uint32_t> rejected_req_indices;

              for (uint64_t i = 0; i < reqs.size(); i++) {
                auto &req = reqs[i];
                auto &[l_key, r_key, shard, data] = req;
                bool success =
                    shard.run(&Shard::try_emplace_batch, l_key, r_key, data);
                if (unlikely(!success)) {
                  rejected_req_indices.push_back(i);
                }
              }
              return rejected_req_indices;
            },
            reqs);

        std::vector<PushDataReq> rejected_reqs;
        for (auto idx : rejected_req_indices) {
          rejected_reqs.emplace_back(std::move(reqs[idx]));
        }

        return rejected_reqs;
      });
}

template <class Container>
void ShardedDataStructure<Container>::handle_rejected_push_reqs(
    std::vector<PushDataReq> &reqs) {
  for (auto &[l_key, r_key, shard, rejected_container] : reqs) {
    auto new_mappings =
        mapping_.run(&ShardingMapping::get_shards_in_range, l_key, r_key);

    for (auto &[k, s] : new_mappings) {
      add_cache(std::move(k), s);
    }

    rejected_container.for_all(
        [&](std::pair<const Key, Val> &p) { emplace(std::move(p)); });
  }
}

template <class Container>
void ShardedDataStructure<Container>::flush() {
again:
  std::vector<std::pair<NodeIP, std::vector<PushDataReq>>> all_ip_reqs;

  for (auto &[ip, _] : ip_to_caches_) {
    auto reqs = gen_push_data_reqs(ip);
    if (!reqs.empty()) {
      all_ip_reqs.emplace_back(ip, std::move(reqs));
    }
  }

  bool rejected = false;

  auto wait_fn = [&] {
    auto &rejected_reqs = push_future_.get();
    if (unlikely(!rejected_reqs.empty())) {
      handle_rejected_push_reqs(rejected_reqs);
      rejected = true;
    }
  };

  if (push_future_) {
    wait_fn();
  }
  for (auto &[ip, reqs] : all_ip_reqs) {
    submit_push_data_req(ip, std::move(reqs));
    wait_fn();
  }

  if (unlikely(rejected)) {
    goto again;
  }
}

template <class Container>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container>::for_all(
    void (*fn)(std::pair<const Key, Val> &p, S0s...), S1s &&... states) {
  flush();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<void>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto pair = shard.get_container_ptr();
          auto *container_ptr = pair.second;
          auto lambda = std::bind(fn, std::placeholders::_1, states...);
          container_ptr->for_all(lambda);
        },
        raw_fn, states...));
  }
}

template <class Container>
Container ShardedDataStructure<Container>::collect() {
  flush();

  auto shards = mapping_.run(&ShardingMapping::get_all_shards);
  std::vector<Future<Container>> futures;
  for (auto &shard : shards) {
    futures.emplace_back(shard.run_async(&Shard::get_container));
  }
  Container all;
  for (auto &future : futures) {
    auto &vec = future.get();
    all.merge(vec);
  }
  return all;
}

template <class Container>
template <class Archive>
void ShardedDataStructure<Container>::save(Archive &ar) const {
  ar(mapping_, max_shard_size_, max_cache_size_, node_proxy_shards_);
  ar(key_to_cache_);
}

template <class Container>
template <class Archive>
void ShardedDataStructure<Container>::load(Archive &ar) {
  ar(mapping_, max_shard_size_, max_cache_size_, node_proxy_shards_);
  ar(key_to_cache_);
  for (auto iter = key_to_cache_.begin(); iter != key_to_cache_.end(); iter++) {
    auto ip = Runtime::get_ip_by_proclet_id(iter->second.shard.get_id());
    bind_cache(ip, iter);
  }
}

template <class Container>
ShardedDataStructure<Container>::Cache::Cache() {}

template <class Container>
ShardedDataStructure<Container>::Cache::Cache(
    WeakProclet<GeneralShard<Container>> s)
    : shard(s) {}

template <class Container>
template <class Archive>
void ShardedDataStructure<Container>::Cache::save(Archive &ar) const {
  ar(shard);
}

template <class Container>
template <class Archive>
void ShardedDataStructure<Container>::Cache::load(Archive &ar) {
  ar(shard);
}

template <class Container>
template <class Archive>
void ShardedDataStructure<Container>::PushDataReq::serialize(Archive &ar) {
  ar(l_key, r_key, shard, container);
}

}  // namespace nu

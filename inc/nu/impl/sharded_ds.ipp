#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace nu {

template <typename Pair>
inline constexpr uint64_t get_proclet_capacity(uint32_t max_shard_size) {
  return 4 * max_shard_size * sizeof(Pair);
}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_size,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      Container container)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      container_(std::move(container)) {}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_size,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      std::size_t capacity)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      container_(capacity) {}

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
void GeneralShard<Container>::split() {
  auto [mid_k, latter_half_container] = container_.split();
  auto proclet_capacity = get_proclet_capacity<Pair>(max_shard_size_);
  auto new_shard = make_proclet_with_capacity<GeneralShard>(
      proclet_capacity, mapping_, max_shard_size_, mid_k, r_key_,
      latter_half_container);
  r_key_ = mid_k;
  mapping_.run(&ShardingMapping::update_mapping, mid_k, std::move(new_shard));
}

template <class Container>
bool GeneralShard<Container>::bad_range(std::optional<Key> l_key,
                                        std::optional<Key> r_key) {
  return (l_key < l_key_) || (r_key_ && (r_key > r_key_ || !r_key));
}

template <class Container>
bool GeneralShard<Container>::try_emplace(std::optional<Key> l_key,
                                          std::optional<Key> r_key, Pair p) {
  ScopedLock<Mutex> guard(&mutex_);

  if (unlikely(bad_range(std::move(l_key), std::move(r_key)))) {
    return false;
  }

  if (unlikely(container_.size() >= max_shard_size_)) {
    split();
    return false;
  }

  container_.emplace(std::move(p.first), std::move(p.second));

  return true;
}

template <class Container>
bool GeneralShard<Container>::try_emplace_batch(std::optional<Key> l_key,
                                                std::optional<Key> r_key,
                                                Container container) {
  ScopedLock<Mutex> guard(&mutex_);

  if (unlikely(bad_range(std::move(l_key), std::move(r_key)))) {
    return false;
  }

  if (unlikely(container_.size() + container.size() > max_shard_size_)) {
    split();
    return false;
  }

  container_.emplace_batch(std::move(container));

  return true;
}

template <class Container>
std::pair<bool, std::optional<typename GeneralShard<Container>::Val>>
GeneralShard<Container>::find_val(Key k) {
  ScopedLock<Mutex> guard(&mutex_);

  bool bad_range = (k < l_key_) || (r_key_ && k > r_key_);
  if (unlikely(bad_range)) {
    return std::make_pair(false, std::nullopt);
  }

  return std::make_pair(true, container_.find_val(std::move(k)));
}

template <class Shard>
std::vector<std::pair<std::optional<typename Shard::Key>, WeakProclet<Shard>>>
GeneralShardingMapping<Shard>::get_shards_in_range(std::optional<Key> l_key,
                                                   std::optional<Key> r_key) {
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>> shards;

  rw_lock_.reader_lock();
  auto iter = --mapping_.upper_bound(l_key);
  while (iter != mapping_.end() && (!r_key || iter->first < r_key)) {
    shards.emplace_back(iter->first, iter->second.get_weak());
    iter++;
  }
  rw_lock_.reader_unlock();

  return shards;
}

template <class Shard>
std::optional<WeakProclet<Shard>>
GeneralShardingMapping<Shard>::get_shard_for_key(std::optional<Key> key) {
  rw_lock_.reader_lock();
  auto iter = --mapping_.upper_bound(key);
  auto shard = iter->second.get_weak();
  rw_lock_.reader_unlock();
  return shard;
}

template <class Shard>
void GeneralShardingMapping<Shard>::update_mapping(std::optional<Key> k,
                                                   Proclet<Shard> shard) {
  rw_lock_.writer_lock();
  auto ret = mapping_.try_emplace(k, std::move(shard));
  rw_lock_.writer_unlock();
  BUG_ON(!ret.second);
}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>::ShardedDataStructure() {}

template <class Container, bool LowLat>
void ShardedDataStructure<Container, LowLat>::set_shard_and_batch_size() {
  constexpr auto max_shard_bytes =
      LowLat ? kLowLatencyMaxShardBytes : kBatchingMaxShardBytes;
  constexpr auto max_batch_bytes =
      LowLat ? kLowLatencyMaxBatchBytes : kBatchingMaxBatchBytes;
  max_shard_size_ = max_shard_bytes / sizeof(Pair);
  max_batch_size_ = max_batch_bytes / sizeof(Pair);
}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>::ShardedDataStructure(
    std::optional<Hint> hint)
    : mapping_(make_proclet<ShardingMapping>()) {
  set_shard_and_batch_size();
  auto container_capacity = max_shard_size_;

  std::vector<std::optional<Key>> keys;
  std::vector<Future<Proclet<Shard>>> shard_futures;

  keys.push_back(std::nullopt);
  if (hint) {
    auto k = hint->estimated_min_key;
    auto num_shards = (hint->num - 1) / max_shard_size_ + 1;
    for (std::size_t i = 0; i < num_shards; i++) {
      keys.push_back(k);
      hint->key_inc_fn(k, max_shard_size_);
    }
  }

  auto proclet_capacity = get_proclet_capacity<Pair>(max_shard_size_);
  for (auto it = keys.begin(); it != keys.end(); it++) {
    auto curr_key = *it;
    auto next_key = (it + 1) == keys.end() ? std::optional<Key>() : *(it + 1);
    shard_futures.emplace_back(make_proclet_async_with_capacity<Shard>(
        proclet_capacity, mapping_.get_weak(), max_shard_size_, curr_key,
        next_key, container_capacity));
  }

  for (std::size_t i = 0; i < keys.size(); i++) {
    auto &shard = shard_futures[i].get();
    auto weak_shard = shard.get_weak();
    auto &key = keys[i];
    auto update_future = mapping_.run_async(&ShardingMapping::update_mapping,
                                            key, std::move(shard));
    auto ret = key_to_shards_.try_emplace(
        key, std::make_pair(weak_shard, Container()));
    BUG_ON(!ret.second);
  }
}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>::ShardedDataStructure(
    const ShardedDataStructure &o)
    : mapping_(o.mapping_),
      max_shard_size_(o.max_shard_size_),
      max_batch_size_(o.max_batch_size_),
      key_to_shards_(o.key_to_shards_) {}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>
    &ShardedDataStructure<Container, LowLat>::operator=(
        const ShardedDataStructure &o) {
  flush();

  mapping_ = o.mapping_;
  max_shard_size_ = o.max_shard_size_;
  max_batch_size_ = o.max_batch_size_;
  key_to_shards_ = o.key_to_shards_;
  return *this;
}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>::ShardedDataStructure(
    ShardedDataStructure &&o) noexcept
    : mapping_(std::move(o.mapping_)),
      max_shard_size_(o.max_shard_size_),
      max_batch_size_(o.max_batch_size_),
      key_to_shards_(std::move(o.key_to_shards_)),
      flush_future_(std::move(o.flush_future_)) {}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>
    &ShardedDataStructure<Container, LowLat>::operator=(
        ShardedDataStructure &&o) noexcept {
  flush();

  mapping_ = std::move(o.mapping_);
  max_shard_size_ = o.max_shard_size_;
  max_batch_size_ = o.max_batch_size_;
  key_to_shards_ = std::move(o.key_to_shards_);
  flush_future_ = std::move(o.flush_future_);
  return *this;
}

template <class Container, bool LowLat>
ShardedDataStructure<Container, LowLat>::~ShardedDataStructure() {
  flush();
}

template <class Container, bool LowLat>
template <typename K1, typename V1>
void ShardedDataStructure<Container, LowLat>::emplace(K1 &&k, V1 &&v) {
  emplace({std::forward<K1>(k), std::forward<V1>(v)});
}

template <class Container, bool LowLat>
void ShardedDataStructure<Container, LowLat>::emplace(Pair &&p) {
  [[maybe_unused]] retry : auto iter = --key_to_shards_.upper_bound(p.first);

  if constexpr (LowLat) {
    auto [l_key, r_key] = get_key_range(iter);
    auto shard = iter->second.first;
    auto right_shard = shard.run(&Shard::try_emplace, l_key, r_key, p);

    if (unlikely(!right_shard)) {
      sync_mapping(l_key, r_key);
      goto retry;
    }
  } else {
    auto &batch = iter->second.second;
    batch.emplace(std::move(p.first), std::move(p.second));

    if (unlikely(batch.size() >= max_batch_size_)) {
      flush_one_batch(iter);
    }
  }
}

template <class Container, bool LowLat>
std::optional<typename ShardedDataStructure<Container, LowLat>::Val>
ShardedDataStructure<Container, LowLat>::find_val(Key k) {
  flush();

retry:
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.first;
  auto [right_shard, val] = shard.run(&Shard::find_val, k);

  if (unlikely(!right_shard)) {
    auto [l_key, r_key] = get_key_range(iter);
    sync_mapping(l_key, r_key);
    goto retry;
  }

  return val;
}

template <class Container, bool LowLat>
std::pair<std::optional<typename ShardedDataStructure<Container, LowLat>::Key>,
          std::optional<typename ShardedDataStructure<Container, LowLat>::Key>>
ShardedDataStructure<Container, LowLat>::get_key_range(
    KeyToShardsMapping::iterator iter) {
  auto l_key = iter->first;
  auto r_key =
      (++iter != key_to_shards_.end()) ? iter->first : std::optional<Key>();
  return std::make_pair(l_key, r_key);
}

template <class Container, bool LowLat>
bool ShardedDataStructure<Container, LowLat>::flush_one_batch(
    KeyToShardsMapping::iterator iter) {
  FlushBatchReq req;
  req.shard = iter->second.first;
  req.batch = std::move(iter->second.second);
  iter->second.second.clear();
  std::tie(req.l_key, req.r_key) = get_key_range(iter);

  bool last_req_succeed = true;
  std::optional<FlushBatchReq> rejected_req;
  if (flush_future_) {
    rejected_req = std::move(flush_future_.get());
  }

  flush_future_ = nu::async([req = std::move(req)]() mutable {
    bool success = req.shard.run(&Shard::try_emplace_batch, req.l_key,
                                 req.r_key, req.batch);
    return success ? std::optional<FlushBatchReq>() : req;
  });

  if (rejected_req) {
    handle_rejected_flush_req(*rejected_req);
    last_req_succeed = false;
  }
  return last_req_succeed;
}

template <class Container, bool LowLat>
void ShardedDataStructure<Container, LowLat>::handle_rejected_flush_req(
    FlushBatchReq &req) {
  sync_mapping(req.l_key, req.r_key);

  auto fn = +[](const Key &key, Val &val, ShardedDataStructure *ds) {
    auto &mut_key = const_cast<Key &>(key);
    ds->emplace(std::move(mut_key), std::move(val));
  };
  req.batch.for_all(fn, this);
}

template <class Container, bool LowLat>
void ShardedDataStructure<Container, LowLat>::flush() {
  if constexpr (!LowLat) {
  retry:
    bool succeed = true;
    for (auto iter = key_to_shards_.begin(); iter != key_to_shards_.end();
         iter++) {
      auto &batch = iter->second.second;
      if (!batch.empty()) {
        succeed &= flush_one_batch(iter);
      }
    }
    if (!key_to_shards_.empty()) {
      succeed &= flush_one_batch(key_to_shards_.begin());
    }

    if (unlikely(!succeed)) {
      goto retry;
    }
  }
}

template <class Container, bool LowLat>
void ShardedDataStructure<Container, LowLat>::sync_mapping(
    std::optional<Key> l_key, std::optional<Key> r_key) {
  auto latest_mapping =
      mapping_.run(&ShardingMapping::get_shards_in_range, l_key, r_key);
  for (auto &[k, s] : latest_mapping) {
    key_to_shards_.try_emplace(k, std::make_pair(s, Container()));
  }
}

template <class Container, bool LowLat>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LowLat>::for_all(
    void (*fn)(const Key &key, Val &val, S0s...), S1s &&... states) {
  flush();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<void>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto pair = shard.get_container_ptr();
          auto *container_ptr = pair.second;
          container_ptr->for_all(fn, states...);
        },
        raw_fn, states...));
  }
}

template <class Container, bool LowLat>
Container ShardedDataStructure<Container, LowLat>::collect() {
  flush();

  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<Container>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(&Shard::get_container));
  }

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get().size();
  }

  Container all(size);
  for (auto &future : futures) {
    all.emplace_batch(std::move(future.get()));
  }

  return all;
}

template <class Container, bool LowLat>
std::size_t ShardedDataStructure<Container, LowLat>::size() {
  flush();

  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<std::size_t>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(
        +[](Shard &s) { return s.get_container_ptr().second->size(); }));
  }

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get();
  }

  return size;
}

template <class Container, bool LowLat>
void ShardedDataStructure<Container, LowLat>::clear() {
  flush();

  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<void>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(
        +[](Shard &s) { s.get_container_ptr().second->clear(); }));
  }

  for (auto &future : futures) {
    future.get();
  }
}

template <class Container, bool LowLat>
template <class Archive>
void ShardedDataStructure<Container, LowLat>::serialize(Archive &ar) {
  ar(mapping_, max_shard_size_, max_batch_size_, key_to_shards_);
}

template <class Container, bool LowLat>
template <class Archive>
void ShardedDataStructure<Container, LowLat>::FlushBatchReq::serialize(
    Archive &ar) {
  ar(l_key, r_key, shard, batch);
}

}  // namespace nu

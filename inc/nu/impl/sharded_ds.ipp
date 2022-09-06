#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace nu {

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure() {}

// TODO: need improvements.
template <class Container, class LL>
void ShardedDataStructure<Container, LL>::set_shard_and_batch_size() {
  constexpr auto max_shard_bytes =
      LL::value ? kLowLatencyMaxShardBytes : kBatchingMaxShardBytes;
  constexpr auto max_batch_bytes =
      LL::value ? kLowLatencyMaxBatchBytes : kBatchingMaxBatchBytes;
  max_shard_size_ = max_shard_bytes / sizeof(Pair);
  max_batch_size_ = max_batch_bytes / sizeof(Pair);
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
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
        key, std::make_pair(weak_shard, std::vector<ContainerReq<Key, Val>>()));
    BUG_ON(!ret.second);
  }
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    const ShardedDataStructure &o)
    : mapping_(o.mapping_),
      max_shard_size_(o.max_shard_size_),
      max_batch_size_(o.max_batch_size_),
      key_to_shards_(o.key_to_shards_) {
  mapping_.run(&GeneralShardingMapping<Shard>::inc_ref_cnt);
}

template <class Container, class LL>
ShardedDataStructure<Container, LL> &
ShardedDataStructure<Container, LL>::operator=(const ShardedDataStructure &o) {
  reset();

  mapping_ = o.mapping_;
  max_shard_size_ = o.max_shard_size_;
  max_batch_size_ = o.max_batch_size_;
  key_to_shards_ = o.key_to_shards_;

  mapping_.run(&GeneralShardingMapping<Shard>::inc_ref_cnt);

  return *this;
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    ShardedDataStructure &&o) noexcept
    : mapping_(std::move(o.mapping_)),
      max_shard_size_(o.max_shard_size_),
      max_batch_size_(o.max_batch_size_),
      key_to_shards_(std::move(o.key_to_shards_)),
      flush_future_(std::move(o.flush_future_)) {}

template <class Container, class LL>
ShardedDataStructure<Container, LL>
    &ShardedDataStructure<Container, LL>::operator=(
        ShardedDataStructure &&o) noexcept {
  reset();

  mapping_ = std::move(o.mapping_);
  max_shard_size_ = o.max_shard_size_;
  max_batch_size_ = o.max_batch_size_;
  key_to_shards_ = std::move(o.key_to_shards_);
  flush_future_ = std::move(o.flush_future_);
  return *this;
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::reset() {
  if (mapping_) {
    flush();
    mapping_.run(&GeneralShardingMapping<Shard>::dec_ref_cnt);
  }
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::~ShardedDataStructure() {
  reset();
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::emplace(Key k, Val v) {
  emplace({std::move(k), std::move(v)});
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::emplace(Pair p) {
[[maybe_unused]] retry:
  auto iter = --key_to_shards_.upper_bound(p.first);

  if constexpr (LL::value) {
    auto [l_key, r_key] = get_key_range(iter);
    auto shard = iter->second.first;
    auto right_shard = shard.run(&Shard::try_emplace, l_key, r_key, p);

    if (unlikely(!right_shard)) {
      sync_mapping(l_key, r_key);
      goto retry;
    }
  } else {
    auto &reqs = iter->second.second;
    reqs.emplace_back(ContainerReq<Key, Val>{Emplace, std::move(p.first),
                                             std::move(p.second)});

    if (unlikely(reqs.size() >= max_batch_size_)) {
      flush_one_batch(iter);
    }
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::emplace_back(
    Val v) requires EmplaceBackAble<Container> {
  [[maybe_unused]] retry :
      // rbegin() is O(1) which is much faster than the O(logn) of --end().
      auto iter = key_to_shards_.rbegin();

  if constexpr (LL::value) {
    auto l_key = iter->first;
    auto r_key = std::optional<Key>();
    auto shard = iter->second.first;
    auto right_shard = shard.run(&Shard::try_emplace_back, l_key, r_key, v);

    if (unlikely(!right_shard)) {
      sync_mapping(l_key, r_key);
      goto retry;
    }
  } else {
    auto &reqs = iter->second.second;
    reqs.emplace_back(ContainerReq<Key, Val>{EmplaceBack, Key(), std::move(v)});

    if (unlikely(reqs.size() >= max_batch_size_)) {
      auto iter = --key_to_shards_.end();
      flush_one_batch(iter);
    }
  }
}

template <class Container, class LL>
std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::__find_val(
    Key k) requires Findable<Container> {
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

template <class Container, class LL>
std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::find_val(Key k) const
    requires Findable<Container> {
  return const_cast<ShardedDataStructure *>(this)->__find_val(k);
}

template <class Container, class LL>
std::pair<std::optional<typename ShardedDataStructure<Container, LL>::Key>,
          std::optional<typename ShardedDataStructure<Container, LL>::Key>>
ShardedDataStructure<Container, LL>::get_key_range(
    KeyToShardsMapping::iterator iter) {
  auto l_key = iter->first;
  auto r_key =
      (++iter != key_to_shards_.end()) ? iter->first : std::optional<Key>();
  return std::make_pair(l_key, r_key);
}

template <class Container, class LL>
bool ShardedDataStructure<Container, LL>::flush_one_batch(
    KeyToShardsMapping::iterator iter) {
  ReqBatch batch;
  batch.shard = iter->second.first;
  batch.reqs = std::move(iter->second.second);
  iter->second.second.clear();
  std::tie(batch.l_key, batch.r_key) = get_key_range(iter);

  bool last_batch_succeed = true;
  std::optional<ReqBatch> rejected_batch;
  if (flush_future_) {
    rejected_batch = std::move(flush_future_.get());
  }

  flush_future_ = nu::async([batch = std::move(batch)]() mutable {
    bool success = batch.shard.run(&Shard::try_handle_batch, batch.l_key,
                                   batch.r_key, batch.reqs);
    return success ? std::optional<ReqBatch>() : batch;
  });

  if (rejected_batch) {
    handle_rejected_flush_batch(*rejected_batch);
    last_batch_succeed = false;
  }
  return last_batch_succeed;
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::handle_rejected_flush_batch(
    ReqBatch &batch) {
  sync_mapping(batch.l_key, batch.r_key);

  for (auto &req : batch.reqs) {
    if (req.type == Emplace) {
      emplace(std::move(req.k), std::move(req.v));
    } else if (req.type == EmplaceBack) {
      if constexpr (EmplaceBackAble<Container>) {
        emplace_back(std::move(req.v));
      }
    }
    else {
      BUG();
    }
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::flush() {
  if constexpr (!LL::value) {
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

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::sync_mapping(
    std::optional<Key> l_key, std::optional<Key> r_key) {
  auto latest_mapping =
      mapping_.run(&ShardingMapping::get_shards_in_range, l_key, r_key);
  for (auto &[k, s] : latest_mapping) {
    key_to_shards_.try_emplace(
        k, std::make_pair(s, std::vector<ContainerReq<Key, Val>>()));
  }
}

template <class Container, class LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all(void (*fn)(const Key &key,
                                                             Val &val, S0s...),
                                                  S1s &&... states) {
  flush();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<void>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto container_ptr = shard.get_container_handle();
          container_ptr->for_all(fn, states...);
        },
        raw_fn, states...));
  }
}

template <class Container, class LL>
Container ShardedDataStructure<Container, LL>::collect() {
  flush();

  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<Container>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(&Shard::get_container_copy));
  }

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get().size();
  }

  Container all(size);
  for (auto &future : futures) {
    all.merge(std::move(future.get()));
  }

  return all;
}

template <class Container, class LL>
std::size_t ShardedDataStructure<Container, LL>::__size() {
  flush();

  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<std::size_t>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(p.first.run_async(
        +[](Shard &s) { return s.get_container_handle()->size(); }));
  }

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get();
  }

  return size;
}

template <class Container, class LL>
std::size_t ShardedDataStructure<Container, LL>::size() const {
  return const_cast<ShardedDataStructure *>(this)->__size();
}

template <class Container, class LL>
bool ShardedDataStructure<Container, LL>::empty() const {
  return !size();
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::clear() {
  flush();

  sync_mapping(std::nullopt, std::nullopt);
  std::vector<Future<void>> futures;
  for (auto &[_, p] : key_to_shards_) {
    futures.emplace_back(
        p.first.run_async(+[](Shard &s) { s.get_container_handle()->clear(); }));
  }

  for (auto &future : futures) {
    future.get();
  }
}

template <class Container, class LL>
template <class Archive>
void ShardedDataStructure<Container, LL>::save(Archive &ar) const {
  ar(mapping_, max_shard_size_, max_batch_size_, key_to_shards_);
}

template <class Container, class LL>
template <class Archive>
void ShardedDataStructure<Container, LL>::load(Archive &ar) {
  ar(mapping_, max_shard_size_, max_batch_size_, key_to_shards_);
  mapping_.run(&GeneralShardingMapping<Shard>::inc_ref_cnt);
}

template <class Container, class LL>
std::pair<std::vector<
              std::optional<typename ShardedDataStructure<Container, LL>::Key>>,
          std::vector<
              WeakProclet<typename ShardedDataStructure<Container, LL>::Shard>>>
ShardedDataStructure<Container, LL>::get_all_non_empty_shards() {
  flush();
  sync_mapping(std::nullopt, std::nullopt);

  std::vector<std::optional<Key>> all_keys;
  std::vector<WeakProclet<Shard>> all_shards;
  std::vector<Future<bool>> shards_emptinesses;
  std::vector<std::optional<Key>> non_empty_keys;
  std::vector<WeakProclet<Shard>> non_empty_shards;

  all_shards.reserve(key_to_shards_.size());
  shards_emptinesses.reserve(key_to_shards_.size());
  for (auto &[k, p] : key_to_shards_) {
    auto &shard = p.first;
    all_keys.emplace_back(k);
    all_shards.emplace_back(shard);
    shards_emptinesses.emplace_back(shard.run_async(&Shard::empty));
  }

  for (uint64_t i = 0; i < all_shards.size(); i++) {
    if (!shards_emptinesses[i].get()) {
      non_empty_keys.push_back(all_keys[i]);
      non_empty_shards.push_back(all_shards[i]);
    }
  }

  return std::make_pair(std::move(non_empty_keys), std::move(non_empty_shards));
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::seal() {
  mapping_.run(&ShardingMapping::seal);
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::unseal() {
  mapping_.run(&ShardingMapping::unseal);
}

}  // namespace nu

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "nu/utils/thread.hpp"

namespace nu {

template <class Container, class LL>
inline ShardedDataStructure<Container, LL>::ShardedDataStructure()
    : max_num_vals_(0), max_num_data_entries_(0) {}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    std::optional<ShardingHint> sharding_hint,
    std::optional<std::size_t> size_bound)
    : max_num_vals_(0), max_num_data_entries_(0) {
  constexpr auto kMaxShardBytes =
      LL::value ? kLowLatencyMaxShardBytes : kBatchingMaxShardBytes;
  constexpr auto kMaxShardSize = kMaxShardBytes / sizeof(DataEntry);

  auto max_shard_count = size_bound.transform([](auto size_bound) {
    return std::max(2UL,  // Have to be at least 2 shards to avoid deadlock.
                    div_round_up_unchecked(
                        size_bound, static_cast<std::size_t>(kMaxShardBytes)));
  });

  mapping_ =
      make_proclet<ShardMapping>(std::tuple(kMaxShardBytes, max_shard_count));

  std::vector<std::optional<Key>> keys;
  std::vector<Future<WeakProclet<Shard>>> shard_futures;
  std::vector<Future<void>> reserve_futures;

  keys.push_back(std::nullopt);
  if (sharding_hint) {
    auto k = sharding_hint->estimated_min_key;
    auto num_shards = (sharding_hint->num - 1) / kMaxShardSize + 1;
    for (std::size_t i = 0; i < num_shards; i++) {
      keys.push_back(k);
      sharding_hint->key_inc_fn(k, kMaxShardSize);
    }
  }

  for (auto it = keys.begin(); it != keys.end(); it++) {
    auto curr_key = *it;
    auto next_key = (it + 1) == keys.end() ? std::optional<Key>() : *(it + 1);

    if constexpr (PushBackAble<Container>) {
      if (!curr_key) {
        shard_futures.emplace_back(
            mapping_.run_async(&ShardMapping::create_new_shard,
                               std::move(curr_key), std::optional<Key>()));
        shard_futures.back().get();
      } else {
        reserve_futures.emplace_back(
            mapping_.run_async(&ShardMapping::reserve_new_shard));
      }
    } else {
      shard_futures.emplace_back(
          mapping_.run_async(&ShardMapping::create_new_shard,
                             std::move(curr_key), std::move(next_key)));
    }
  }

  for (std::size_t i = 0; i < shard_futures.size(); i++) {
    auto &weak_shard = shard_futures[i].get();
    auto &key = keys[i];
    key_to_shards_.emplace(key, ShardAndReqs(weak_shard));
  }
  mapping_seq_ = shard_futures.size() - 1;
}

template <class Container, class LL>
inline ShardedDataStructure<Container, LL>::ShardedDataStructure(
    const ShardedDataStructure &o)
    : mapping_(o.mapping_),
      mapping_seq_(o.mapping_seq_),
      key_to_shards_(o.key_to_shards_),
      max_num_vals_(o.max_num_vals_),
      max_num_data_entries_(o.max_num_data_entries_) {
  mapping_.run(&GeneralShardMapping<Shard>::inc_ref_cnt);
}

template <class Container, class LL>
ShardedDataStructure<Container, LL> &
ShardedDataStructure<Container, LL>::operator=(const ShardedDataStructure &o) {
  reset();
  const_cast<ShardedDataStructure &>(o).flush();

  mapping_ = o.mapping_;
  mapping_seq_ = o.mapping_seq_;
  key_to_shards_ = o.key_to_shards_;
  max_num_vals_ = o.max_num_vals_;
  max_num_data_entries_ = o.max_num_data_entries_;

  mapping_.run(&GeneralShardMapping<Shard>::inc_ref_cnt);

  return *this;
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    ShardedDataStructure &&o) noexcept
    : mapping_(std::move(o.mapping_)),
      mapping_seq_(o.mapping_seq_),
      key_to_shards_(std::move(o.key_to_shards_)),
      push_back_reqs_(std::move(o.push_back_reqs_)),
      flush_futures_(std::move(o.flush_futures_)),
      max_num_vals_(o.max_num_vals_),
      max_num_data_entries_(o.max_num_data_entries_) {}

template <class Container, class LL>
ShardedDataStructure<Container, LL>
    &ShardedDataStructure<Container, LL>::operator=(
        ShardedDataStructure &&o) noexcept {
  reset();

  mapping_ = std::move(o.mapping_);
  mapping_seq_ = o.mapping_seq_;
  key_to_shards_ = std::move(o.key_to_shards_);
  push_back_reqs_ = std::move(o.push_back_reqs_);
  flush_futures_ = std::move(o.flush_futures_);
  max_num_vals_ = o.max_num_vals_;
  max_num_data_entries_ = o.max_num_data_entries_;

  return *this;
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::reset() {
  if (mapping_) {
    flush();
    key_to_shards_.clear();
    mapping_.run(&GeneralShardMapping<Shard>::dec_ref_cnt);
  }
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::~ShardedDataStructure() {
  reset();
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::update_max_num_data_entries(
    KeyToShardsMapping::iterator iter) {
  max_num_data_entries_ = kBatchingMaxBatchBytes /
                          cereal::get_size(iter->second.insert_reqs.back());
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::update_max_num_vals() {
  max_num_vals_ =
      kBatchingMaxBatchBytes / cereal::get_size(push_back_reqs_.back());
}

template <class Container, class LL>
template <typename K, typename V>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::insert(
    K &&k, V &&v) requires(HasVal<Container> && InsertAble<Container>) {
  insert({std::forward<K>(k), std::forward<V>(v)});
}

template <class Container, class LL>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::insert(
    const DataEntry &entry) requires InsertAble<Container> {
  __insert(const_cast<DataEntry &>(entry));
}

template <class Container, class LL>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::insert(
    DataEntry &&entry) requires InsertAble<Container> {
  __insert(std::move(entry));
}

template <class Container, class LL>
template <typename D>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::__insert(
    D &&entry) requires InsertAble<Container> {
[[maybe_unused]] retry:
  typename KeyToShardsMapping::iterator iter;
  if constexpr (HasVal<Container>) {
    iter = --key_to_shards_.upper_bound(entry.first);
  } else {
    iter = --key_to_shards_.upper_bound(entry);
  }

  if constexpr (LL::value) {
    auto [l_key, r_key] = get_key_range(iter);
    auto shard = iter->second.shard;
    auto succeed =
        shard.run(&Shard::try_insert, l_key, r_key, entry);

    if (unlikely(!succeed)) {
      sync_mapping();
      goto retry;
    }
  } else {
    auto &reqs = iter->second.insert_reqs;
    reqs.emplace_back(std::forward<D>(entry));

    if (unlikely(reqs.size() >= max_num_data_entries_)) {
      update_max_num_data_entries(iter);
      flush_one_batch(iter, /* drain = */ false);
    }
  }
}

template <class Container, class LL>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::push_back(
    const Val &v) requires PushBackAble<Container> {
  __push_back(const_cast<Val &>(v));
}

template <class Container, class LL>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::push_back(
    Val &&v) requires PushBackAble<Container> {
  __push_back(std::move(v));
}

template <class Container, class LL>
template <typename V>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::__push_back(
    V &&v) requires PushBackAble<Container> {
[[maybe_unused]] retry:
  if constexpr (LL::value) {
    run_at_border<false, void>(&Shard::try_push_back, std::forward<V>(v));
  } else {
    push_back_reqs_.emplace_back(std::forward<V>(v));

    if (unlikely(push_back_reqs_.size() >= max_num_vals_)) {
      update_max_num_vals();
      auto iter = --key_to_shards_.end();
      flush_one_batch(iter, /* drain = */ false);
    }
  }
}

template <class Container, class LL>
template <bool Front, typename RetT, typename F, typename... As>
RetT ShardedDataStructure<Container, LL>::run_at_border(F f, As &&... args) {
retry:
  std::optional<Key> l_key, r_key;
  WeakProclet<Shard> shard;

  if constexpr (Front) {
    auto iter = key_to_shards_.begin();
    shard = iter->second.shard;
    l_key = iter->first;
    // TODO: memoize r_key to avoid the O(logn)'s ++iter.
    r_key =
        (++iter != key_to_shards_.end()) ? iter->first : std::optional<Key>();
  } else {
    auto iter = key_to_shards_.rbegin();
    shard = iter->second.shard;
    l_key = iter->first;
    r_key = std::optional<Key>();
  }

  auto val = shard.run(f, l_key, r_key, args...);
  if (unlikely(!val)) {
    sync_mapping();
    goto retry;
  }

  if constexpr (!std::is_same_v<RetT, void>) {
    return *val;
  }
}

template <class Container, class LL>
inline Container::Val ShardedDataStructure<Container, LL>::front() const
    requires HasFront<Container> {
  return const_cast<ShardedDataStructure *>(this)->__front();
}

template <class Container, class LL>
inline Container::Val
ShardedDataStructure<Container, LL>::__front() requires HasFront<Container> {
  return run_at_border<true, Val>(&Shard::try_front);
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::push_front(
    const Val &v) requires PushFrontAble<Container> {
  __push_front(const_cast<Val &>(v));
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::push_front(
    Val &&v) requires PushFrontAble<Container> {
  __push_front(std::move(v));
}

template <class Container, class LL>
template <typename V>
inline void ShardedDataStructure<Container, LL>::__push_front(
    V &&v) requires PushFrontAble<Container> {
[[maybe_unused]] retry:
  if constexpr (LL::value) {
    run_at_border<true, void>(&Shard::try_push_front, std::forward<V>(v));
  } else {
    // TODO: implement batching.
    BUG();
  }
}

template <class Container, class LL>
inline Container::Val ShardedDataStructure<
    Container, LL>::pop_front() requires TryPopFrontAble<Container> {
  return run_at_border<true, Val>(&Shard::try_pop_front);
}

template <class Container, class LL>
inline std::vector<typename Container::Val>
ShardedDataStructure<Container, LL>::try_pop_front(
    std::size_t num) requires TryPopFrontAble<Container> {
  return run_at_border<true, std::vector<Val>>(&Shard::try_pop_front_nb, num);
}

template <class Container, class LL>
inline Container::Val ShardedDataStructure<Container, LL>::back() const
    requires HasBack<Container> {
  return const_cast<ShardedDataStructure *>(this)->__back();
}

template <class Container, class LL>
inline Container::Val
ShardedDataStructure<Container, LL>::__back() requires HasBack<Container> {
  return run_at_border<false, Val>(&Shard::try_back);
}

template <class Container, class LL>
inline Container::Val ShardedDataStructure<
    Container, LL>::pop_back() requires TryPopBackAble<Container> {
  return run_at_border<false, Val>(&Shard::try_pop_back);
}

template <class Container, class LL>
inline std::vector<typename Container::Val>
ShardedDataStructure<Container, LL>::try_pop_back(
    std::size_t num) requires TryPopBackAble<Container> {
  return run_at_border<true, std::vector<Val>>(&Shard::try_pop_back_nb, num);
}

template <class Container, class LL>
std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::__find_data(
    Key k) requires FindAble<Container> {
  flush();

retry:
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.shard;
  auto [succeed, val] = shard.run(&Shard::find_data, k);

  if (unlikely(!succeed)) {
    auto [l_key, r_key] = get_key_range(iter);
    sync_mapping();
    goto retry;
  }

  return val;
}

template <class Container, class LL>
inline std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::find_data(Key k) const
    requires FindAble<Container> {
  return const_cast<ShardedDataStructure *>(this)->__find_data(k);
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::concat(
    ShardedDataStructure &&tail) requires Container::kContiguousIterator {
  flush();
  tail.flush();
  mapping_.run_async(&ShardMapping::concat, tail.mapping_.get_weak());
  tail.key_to_shards_.clear();
}

template <class Container, class LL>
inline std::pair<
    std::optional<typename ShardedDataStructure<Container, LL>::Key>,
    std::optional<typename ShardedDataStructure<Container, LL>::Key>>
ShardedDataStructure<Container, LL>::get_key_range(
    KeyToShardsMapping::iterator iter) {
  auto l_key = iter->first;
  auto r_key =
      (++iter != key_to_shards_.end()) ? iter->first : std::optional<Key>();
  return std::make_pair(l_key, r_key);
}

template <class Container, class LL>
inline ShardedDataStructure<Container, LL>::ShardAndReqs::ShardAndReqs(
    WeakProclet<Shard> s)
    : shard(s), seq(0) {}

template <class Container, class LL>
inline ShardedDataStructure<Container, LL>::ShardAndReqs::ShardAndReqs(
    const ShardAndReqs &o)
    : shard(o.shard), seq(0) {}

template <class Container, class LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::ShardAndReqs::save(
    Archive &ar) const {
  ar(shard);
}

template <class Container, class LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::ShardAndReqs::load(
    Archive &ar) {
  ar(shard);
  seq = 0;
}

template <class Container, class LL>
bool ShardedDataStructure<Container, LL>::flush_one_batch(
    KeyToShardsMapping::iterator iter, bool drain) {
  ReqBatch batch;

  auto &shard_and_reqs = iter->second;
  batch.mapping_seq = mapping_seq_;
  if (iter == --key_to_shards_.end()) {
    batch.push_back_reqs = std::move(push_back_reqs_);
    push_back_reqs_.clear();
    push_back_reqs_.reserve(batch.push_back_reqs.size());
  }
  batch.insert_reqs = std::move(shard_and_reqs.insert_reqs);
  shard_and_reqs.insert_reqs.clear();
  std::tie(batch.l_key, batch.r_key) = get_key_range(iter);

  if (unlikely(!shard_and_reqs.flush_executor)) {
    shard_and_reqs.flush_executor = shard_and_reqs.shard.run(+[](Shard &s) {
      return make_rem_unique<RobExecutor<ReqBatch, std::optional<ReqBatch>>>(
          [&](ReqBatch &&batch) { return s.try_handle_batch(batch); },
          kMaxNumInflightFlushes);
    });
  }

  std::vector<ReqBatch> rejected_batches;
  bool reject_cur_flush = false;

  auto pop_flush_futures = [&] {
    auto &optional_batch = flush_futures_.front().get();
    if (optional_batch) {
      drain = true;
      reject_cur_flush |= (optional_batch->l_key == batch.l_key &&
                           optional_batch->r_key == batch.r_key);
      rejected_batches.emplace_back(std::move(*optional_batch));
    }
    flush_futures_.pop();
  };

  BUG_ON(flush_futures_.size() > kMaxNumInflightFlushes);
  while (!flush_futures_.empty() &&
         (drain || flush_futures_.front().is_ready() ||
          flush_futures_.size() == kMaxNumInflightFlushes)) {
    pop_flush_futures();
  }

  if (!batch.empty()) {
    if (!reject_cur_flush) {
      flush_futures_.emplace(shard_and_reqs.flush_executor.run_async(
          +[](RobExecutor<ReqBatch, std::optional<ReqBatch>> &rob_executor,
              uint32_t rob_seq, ReqBatch batch) {
            return rob_executor.submit(rob_seq, std::move(batch));
          },
          shard_and_reqs.seq++, std::move(batch)));
      if (drain) {
        pop_flush_futures();
      }
    } else {
      rejected_batches.emplace_back(std::move(batch));
    }
  }

  if (!rejected_batches.empty()) {
    handle_rejected_flush_batches(std::move(rejected_batches));
    return false;
  } else {
    return true;
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::reroute_reqs(
    std::vector<DataEntry> insert_reqs, std::vector<Val> push_back_reqs) {
  if constexpr (InsertAble<Container>) {
    for (auto &req : insert_reqs) {
      insert(std::move(req));
    }
  }
  if constexpr (PushBackAble<Container>) {
    for (auto &req : push_back_reqs) {
      push_back(std::move(req));
    }
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::handle_rejected_flush_batches(
    std::vector<ReqBatch> batches) {
  auto [last_insert_reqs, last_push_back_reqs] =
      sync_mapping(/* dont_reroute = */ true);
  for (auto &batch : batches) {
    reroute_reqs(std::move(batch.insert_reqs), std::move(batch.push_back_reqs));
  }
  reroute_reqs(std::move(last_insert_reqs), std::move(last_push_back_reqs));
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::flush() {
  if constexpr (!LL::value) {
  again:
    for (auto iter = key_to_shards_.begin(); iter != key_to_shards_.end();
         iter++) {
      if (unlikely(!flush_one_batch(iter, /* drain = */ true))) {
        goto again;
      }
    }
  }
}

template <class Container, class LL>
std::pair<std::vector<typename ShardedDataStructure<Container, LL>::DataEntry>,
          std::vector<typename ShardedDataStructure<Container, LL>::Val>>
ShardedDataStructure<Container, LL>::sync_mapping(bool dont_reroute) {
  std::vector<DataEntry> insert_reqs;
  uint64_t latest_seq = 0;

  auto v = mapping_.run(&ShardMapping::get_updates, mapping_seq_ + 1);

  typename ShardMapping::LogUpdates *updates;
  if (likely(updates = std::get_if<typename ShardMapping::LogUpdates>(&v))) {
    for (auto &entry : *updates) {
      if (entry.op == LogEntry<Shard>::kInsert) {
        auto it = key_to_shards_.emplace(entry.l_key, entry.shard);
        if (it != key_to_shards_.begin()) {
          move_append_vector(insert_reqs, (--it)->second.insert_reqs);
        }
      } else if (entry.op == LogEntry<Shard>::kDelete) {
        auto [begin_it, end_it] = key_to_shards_.equal_range(entry.l_key);
        auto it = begin_it;
        for (; it != end_it; ++it) {
          if (it->second.shard == entry.shard) {
            break;
          }
        }
        BUG_ON(it == end_it);

        move_append_vector(insert_reqs, it->second.insert_reqs);
        key_to_shards_.erase(it);
      } else {
        BUG();
      }
    }

    if (!updates->empty()) {
      latest_seq = updates->back().seq;
    }
  } else {
    auto &snapshot = std::get<typename ShardMapping::Snapshot>(v);
    for (auto &[k, s] : key_to_shards_) {
      move_append_vector(insert_reqs, s.insert_reqs);
    }
    key_to_shards_.clear();
    for (auto &[k, s] : snapshot.second) {
      key_to_shards_.emplace(k, std::move(s));
    }
    latest_seq = snapshot.first;
  }

  if (latest_seq) {
    BUG_ON(mapping_seq_ > latest_seq);
    mapping_seq_ = latest_seq;
  }
  auto push_back_reqs = std::move(push_back_reqs_);
  push_back_reqs_.clear();

  if (!dont_reroute) {
    reroute_reqs(std::move(insert_reqs), std::move(push_back_reqs));
    return std::make_pair(std::vector<DataEntry>(), std::vector<Val>());
  } else {
    return std::make_pair(std::move(insert_reqs), std::move(push_back_reqs));
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::flush_and_sync_mapping() {
  flush();

  auto latest_mapping = mapping_.run(&ShardMapping::get_all_keys_and_shards);
  key_to_shards_.clear();
  for (auto &[k, s] : latest_mapping) {
    key_to_shards_.emplace(k, ShardAndReqs(s));
  }
}

template <class Container, class LL>
template <typename... S1s>
void ShardedDataStructure<Container, LL>::__for_all(auto *fn,
                                                    S1s &&... states) {
  flush_and_sync_mapping();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  std::vector<Future<void>> futures;
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(shard_and_reqs.shard.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto container_ptr = shard.get_container_handle();
          container_ptr->for_all(fn, states...);
        },
        raw_fn, states...));
  }
}

template <class Container, class LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all(
    void (*fn)(const Key &key, Val &val, S0s...),
    S1s &&... states) requires HasVal<Container> {
  __for_all(fn, std::forward<S1s>(states)...);
}

template <class Container, class LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all(
    void (*fn)(const Key &key, S0s...),
    S1s &&... states) requires(!HasVal<Container>) {
  __for_all(fn, std::forward<S1s>(states)...);
}

template <class Container, class LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all_shards(
    void (*fn)(ContainerImpl &container_impl, S0s...), S1s &&... states) {
  flush_and_sync_mapping();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  std::vector<Future<void>> futures;
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(shard_and_reqs.shard.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S1s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto container_ptr = shard.get_container_handle();
          container_ptr->pass_through(fn, states...);
        },
        raw_fn, states...));
  }
}

template <class Container, class LL>
Container ShardedDataStructure<Container, LL>::collect() {
  flush_and_sync_mapping();

  std::vector<Future<Container>> futures;
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(
        shard_and_reqs.shard.template run_async</* MigrEn = */ false>(
            &Shard::get_container_copy));
  }

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get().size();
  }

  Container all;
  if constexpr (Reservable<Container>) {
    all.reserve(size);
  }
  for (auto &future : futures) {
    all.merge(std::move(future.get()));
  }

  return all;
}

template <class Container, class LL>
std::size_t ShardedDataStructure<Container, LL>::__size() {
  flush_and_sync_mapping();

  std::vector<Future<std::size_t>> futures;
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(shard_and_reqs.shard.run_async(
        +[](Shard &s) { return s.get_container_handle()->size(); }));
  }

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get();
  }

  return size;
}

template <class Container, class LL>
inline std::size_t ShardedDataStructure<Container, LL>::size() const {
  return const_cast<ShardedDataStructure *>(this)->__size();
}

template <class Container, class LL>
inline bool ShardedDataStructure<Container, LL>::empty() const {
  return !size();
}

template <class Container, class LL>
void ShardedDataStructure<Container,
                          LL>::clear() requires ClearAble<Container> {
  flush_and_sync_mapping();

  std::vector<Future<void>> futures;
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(shard_and_reqs.shard.run_async(
        +[](Shard &s) { s.get_container_handle()->clear(); }));
  }

  for (auto &future : futures) {
    future.get();
  }
}

template <class Container, class LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::__save(Archive &ar) {
  flush();
  ar(mapping_, mapping_seq_, key_to_shards_, max_num_vals_,
     max_num_data_entries_);
  mapping_.run(&GeneralShardMapping<Shard>::inc_ref_cnt);
}

template <class Container, class LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::save(Archive &ar) const {
  const_cast<ShardedDataStructure *>(this)->__save(ar);
}

template <class Container, class LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::load(Archive &ar) {
  ar(mapping_, mapping_seq_, key_to_shards_, max_num_vals_,
     max_num_data_entries_);
}

template <class Container, class LL>
std::vector<std::tuple<
    std::optional<typename ShardedDataStructure<Container, LL>::Key>,
    std::size_t,
    WeakProclet<typename ShardedDataStructure<Container, LL>::Shard>>>
ShardedDataStructure<Container, LL>::get_all_shards_info() {
  flush_and_sync_mapping();

  std::vector<std::tuple<std::optional<Key>, std::size_t, WeakProclet<Shard>>>
      ret;
  std::vector<nu::Thread> ths;

  ret.reserve(key_to_shards_.size());
  ths.reserve(key_to_shards_.size());
  for (auto &[k, shard_and_reqs] : key_to_shards_) {
    auto shard = shard_and_reqs.shard.get_weak();
    ret.emplace_back(k, 0, shard);
    auto *size_ptr = &std::get<1>(ret.back());
    ths.emplace_back(
        [size_ptr, shard]() mutable { *size_ptr = shard.run(&Shard::size); });
  }

  for (auto &th : ths) {
    th.join();
  }

  return ret;
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::seal() {
  mapping_.run(&ShardMapping::seal);
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::unseal() {
  mapping_.run(&ShardMapping::unseal);
}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL>::BackInsertIterator(
    ShardedDataStructure<Container, LL> &ds)
    : ds_(ds) {}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL>
    &BackInsertIterator<Container, LL>::operator++() {
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL>
    &BackInsertIterator<Container, LL>::operator*() {
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
inline BackInsertIterator<Container, LL>
    &BackInsertIterator<Container, LL>::operator=(const Val &val) {
  ds_.push_back(val);
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
inline BackInsertIterator<Container, LL>
    &BackInsertIterator<Container, LL>::operator=(Val &&val) {
  ds_.push_back(std::move(val));
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL> back_inserter(
    ShardedDataStructure<Container, LL> &sharded_ds) {
  return BackInsertIterator(sharded_ds);
};

}  // namespace nu

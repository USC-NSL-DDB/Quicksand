#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "nu/utils/thread.hpp"

namespace nu {

template <class Container, class LL>
inline ShardedDataStructure<Container, LL>::ShardedDataStructure() {}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    std::optional<Hint> hint) {
  constexpr auto kMaxShardBytes =
      LL::value ? kLowLatencyMaxShardBytes : kBatchingMaxShardBytes;
  constexpr auto kMaxShardSize = kMaxShardBytes / sizeof(DataEntry);

  mapping_ = make_proclet<ShardingMapping>(kMaxShardBytes);

  std::vector<std::optional<Key>> keys;
  std::vector<Future<WeakProclet<Shard>>> shard_futures;
  std::vector<Future<void>> reserve_futures;

  keys.push_back(std::nullopt);
  if (hint) {
    auto k = hint->estimated_min_key;
    auto num_shards = (hint->num - 1) / kMaxShardSize + 1;
    for (std::size_t i = 0; i < num_shards; i++) {
      keys.push_back(k);
      hint->key_inc_fn(k, kMaxShardSize);
    }
  }

  for (auto it = keys.begin(); it != keys.end(); it++) {
    auto curr_key = *it;
    auto next_key = (it + 1) == keys.end() ? std::optional<Key>() : *(it + 1);

    if constexpr (EmplaceBackAble<Container>) {
      if (!curr_key) {
        shard_futures.emplace_back(mapping_.run_async(
            &ShardingMapping::create_new_shard, std::move(curr_key),
            std::optional<Key>(), /* reserve_space = */ true));
        shard_futures.back().get();
      } else {
        reserve_futures.emplace_back(
            mapping_.run_async(&ShardingMapping::reserve_new_shard));
      }
    } else {
      shard_futures.emplace_back(mapping_.run_async(
          &ShardingMapping::create_new_shard, std::move(curr_key),
          std::move(next_key), /* reserve_space = */ true));
    }
  }

  for (std::size_t i = 0; i < shard_futures.size(); i++) {
    auto &weak_shard = shard_futures[i].get();
    auto &key = keys[i];
    key_to_shards_.emplace(key, ShardAndReqs(weak_shard));
  }
}

template <class Container, class LL>
inline ShardedDataStructure<Container, LL>::ShardedDataStructure(
    const ShardedDataStructure &o)
    : mapping_(o.mapping_), key_to_shards_(o.key_to_shards_) {
  mapping_.run(&GeneralShardingMapping<Shard>::inc_ref_cnt);
}

template <class Container, class LL>
ShardedDataStructure<Container, LL> &
ShardedDataStructure<Container, LL>::operator=(const ShardedDataStructure &o) {
  reset();
  const_cast<ShardedDataStructure &>(o).flush();

  mapping_ = o.mapping_;
  key_to_shards_ = o.key_to_shards_;

  mapping_.run(&GeneralShardingMapping<Shard>::inc_ref_cnt);

  return *this;
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    ShardedDataStructure &&o) noexcept
    : mapping_(std::move(o.mapping_)),
      key_to_shards_(std::move(o.key_to_shards_)),
      emplace_back_reqs_(std::move(o.emplace_back_reqs_)),
      flush_futures_(std::move(o.flush_futures_)) {}

template <class Container, class LL>
ShardedDataStructure<Container, LL>
    &ShardedDataStructure<Container, LL>::operator=(
        ShardedDataStructure &&o) noexcept {
  reset();

  mapping_ = std::move(o.mapping_);
  key_to_shards_ = std::move(o.key_to_shards_);
  emplace_back_reqs_ = std::move(o.emplace_back_reqs_);
  flush_futures_ = std::move(o.flush_futures_);
  return *this;
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::reset() {
  if (mapping_) {
    flush();
    key_to_shards_.clear();
    mapping_.run(&GeneralShardingMapping<Shard>::dec_ref_cnt);
  }
}

template <class Container, class LL>
ShardedDataStructure<Container, LL>::~ShardedDataStructure() {
  reset();
}

template <class Container, class LL>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::emplace(
    Key k, Val v) requires HasVal<Container> {
  emplace({std::move(k), std::move(v)});
}

template <class Container, class LL>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::emplace(
    DataEntry entry) {
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
    auto succeed = shard.run(&Shard::try_emplace, l_key, r_key, entry);

    if (unlikely(!succeed)) {
      sync_mapping(l_key, r_key, shard);
      goto retry;
    }
  } else {
    auto &reqs = iter->second.emplace_reqs;
    reqs.emplace_back(std::move(entry));

    if (unlikely(reqs.size() >= kBatchingMaxBatchBytes / sizeof(DataEntry))) {
      flush_one_batch(iter, /* drain = */ false);
    }
  }
}

template <class Container, class LL>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::emplace_back(
    Val v) requires EmplaceBackAble<Container> {
[[maybe_unused]] retry:
  if constexpr (LL::value) {
    // rbegin() is O(1) which is much faster than the O(logn) of --end().
    auto iter = key_to_shards_.rbegin();
    auto l_key = iter->first;
    auto r_key = std::optional<Key>();
    auto shard = iter->second.shard;
    auto succeed = shard.run(&Shard::try_emplace_back, l_key, r_key, v);

    if (unlikely(!succeed)) {
      sync_mapping(l_key, r_key, shard);
      goto retry;
    }
  }
  else {
    emplace_back_reqs_.emplace_back(std::move(v));

    if (unlikely(emplace_back_reqs_.size() >=
                 kBatchingMaxBatchBytes / sizeof(Val))) {
      auto iter = --key_to_shards_.end();
      flush_one_batch(iter, /* drain = */ false);
    }
  }
}

// TODO: all front/back operations only implemented for LL so far
template <class Container, class LL>
template <bool Front, typename RetT, typename Func, class... Args>
RetT ShardedDataStructure<Container, LL>::front_back_impl(Func func,
                                                          Args &... args) {
retry:
  std::optional<Key> l_key, r_key;
  WeakProclet<Shard> shard;
  if constexpr (Front) {
    auto iter = key_to_shards_.begin();
    shard = iter->second.shard;
    l_key = std::optional<Key>();
    r_key =
        (++iter != key_to_shards_.end()) ? iter->first : std::optional<Key>();
  } else {
    auto iter = key_to_shards_.rbegin();
    shard = iter->second.shard;
    l_key = iter->first;
    r_key = std::optional<Key>();
  }

  auto val = shard.run(func, l_key, r_key, args...);

  bool succeed;
  if constexpr (!std::is_same_v<RetT, void>) {
    succeed = val.has_value();
  } else {
    succeed = val;
  }

  if (unlikely(!succeed)) {
    sync_mapping(l_key, r_key, shard);
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
  return front_back_impl<true, Val>(&Shard::try_front);
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::emplace_front(
    Val v) requires EmplaceFrontAble<Container> {
  front_back_impl<true, void>(&Shard::try_emplace_front, v);
}

template <class Container, class LL>
inline void ShardedDataStructure<
    Container, LL>::pop_front() requires PopFrontAble<Container> {
  front_back_impl<true, void>(&Shard::try_pop_front);
}

template <class Container, class LL>
inline Container::Val ShardedDataStructure<Container, LL>::back() const
    requires HasBack<Container> {
  return const_cast<ShardedDataStructure *>(this)->__back();
}

template <class Container, class LL>
inline Container::Val
ShardedDataStructure<Container, LL>::__back() requires HasBack<Container> {
  return front_back_impl<false, Val>(&Shard::try_back);
}

template <class Container, class LL>
inline void ShardedDataStructure<
    Container, LL>::pop_back() requires PopBackAble<Container> {
  front_back_impl<false, void>(&Shard::try_pop_back);
}

template <class Container, class LL>
std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::__find_data(
    Key k) requires Findable<Container> {
  flush();

retry:
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.shard;
  auto [succeed, val] = shard.run(&Shard::find_data, k);

  if (unlikely(!succeed)) {
    auto [l_key, r_key] = get_key_range(iter);
    sync_mapping(l_key, r_key, shard);
    goto retry;
  }

  return val;
}

template <class Container, class LL>
inline std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::find_data(Key k) const
    requires Findable<Container> {
  return const_cast<ShardedDataStructure *>(this)->__find_data(k);
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
  batch.shard = shard_and_reqs.shard;
  if (iter == --key_to_shards_.end()) {
    batch.emplace_back_reqs = std::move(emplace_back_reqs_);
    emplace_back_reqs_.clear();
    emplace_back_reqs_.reserve(batch.emplace_back_reqs.size());
  }
  batch.emplace_reqs = std::move(shard_and_reqs.emplace_reqs);
  shard_and_reqs.emplace_reqs.clear();
  std::tie(batch.l_key, batch.r_key) = get_key_range(iter);

  if (unlikely(!shard_and_reqs.flush_executor)) {
    shard_and_reqs.flush_executor = batch.shard.run(+[](Shard &s) {
      return make_rem_unique<RobExecutor<ReqBatch, std::optional<ReqBatch>>>(
          [&](const ReqBatch &batch) { return s.try_handle_batch(batch); },
          kMaxNumInflightFlushes);
    });
  }

  std::vector<ReqBatch> rejected_batches;

  auto pop_flush_futures = [&] {
    auto &optional_batch = flush_futures_.front().get();
    if (optional_batch) {
      drain = true;
      rejected_batches.emplace_back(std::move(*optional_batch));
    }
    flush_futures_.pop();
  };

  if (flush_futures_.size() == kMaxNumInflightFlushes) {
    pop_flush_futures();
  }

  flush_futures_.emplace(shard_and_reqs.flush_executor.run_async(
      +[](RobExecutor<ReqBatch, std::optional<ReqBatch>> &rob_executor,
          uint32_t seq, ReqBatch batch) {
        return rob_executor.submit(seq, std::move(batch));
      },
      shard_and_reqs.seq++, std::move(batch)));

  while (drain && !flush_futures_.empty()) {
    pop_flush_futures();
  }

  for (auto &batch : rejected_batches) {
    handle_rejected_flush_batch(batch);
  }

  return rejected_batches.empty();
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::handle_rejected_flush_batch(
    ReqBatch &batch) {
  sync_mapping(batch.l_key, batch.r_key, batch.shard);

  for (auto &req : batch.emplace_back_reqs) {
    if constexpr (EmplaceBackAble<Container>) {
      emplace_back(std::move(req));
    }
  }
  for (auto &req : batch.emplace_reqs) {
    emplace(std::move(req));
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::flush() {
  if constexpr (!LL::value) {
    for (auto iter = key_to_shards_.begin(); iter != key_to_shards_.end();
         iter++) {
      while (!flush_one_batch(iter, /* drain = */ true))
        ;
    }
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::sync_mapping(
    std::optional<Key> l_key, std::optional<Key> r_key,
    WeakProclet<Shard> shard) {
  auto range = r_key
                   ? key_to_shards_.equal_range(r_key)
                   : std::make_pair(key_to_shards_.end(), key_to_shards_.end());
  auto kts_iter = (l_key != r_key) ? range.first : range.second;
  auto current_shard = (--kts_iter)->second.shard;
  // We've already got a newer mapping.
  if (unlikely(shard != current_shard)) {
    return;
  }

  auto latest_mapping =
      mapping_.run(&ShardingMapping::get_shards_in_range, l_key, r_key);

  auto lm_iter = latest_mapping.begin();
  for (; lm_iter->second != shard; ++lm_iter)
    ;
  for (++lm_iter; lm_iter != latest_mapping.end(); ++lm_iter) {
    auto &[k, s] = *lm_iter;
    key_to_shards_.emplace(k, ShardAndReqs(s));
  }
}

template <class Container, class LL>
void ShardedDataStructure<Container, LL>::flush_and_sync_mapping() {
  flush();

  auto latest_mapping = mapping_.run(&ShardingMapping::get_all_shards);
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
        shard_and_reqs.shard.run_async(&Shard::get_container_copy));
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
inline void ShardedDataStructure<Container, LL>::save(Archive &ar) const {
  const_cast<ShardedDataStructure *>(this)->flush();
  ar(mapping_, key_to_shards_);
}

template <class Container, class LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::load(Archive &ar) {
  ar(mapping_, key_to_shards_);
  mapping_.run(&GeneralShardingMapping<Shard>::inc_ref_cnt);
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
    auto &shard = shard_and_reqs.shard;
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
  mapping_.run(&ShardingMapping::seal);
}

template <class Container, class LL>
inline void ShardedDataStructure<Container, LL>::unseal() {
  mapping_.run(&ShardingMapping::unseal);
}

}  // namespace nu

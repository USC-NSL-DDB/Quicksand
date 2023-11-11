#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "nu/utils/thread.hpp"
#include "nu/utils/time.hpp"

namespace nu {

template <GeneralContainerBased Container, BoolIntegral LL>
inline ShardedDataStructure<Container, LL>::ShardedDataStructure()
    : num_pending_flushes_(0),
      max_num_vals_(0),
      max_num_data_entries_(0),
      rw_lock_(std::make_unique<ReadSkewedLock>()) {}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename... As>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    std::optional<ShardingHint> sharding_hint, std::optional<NodeIP> pinned_ip,
    As &&...args)
    : mapping_seq_(0),
      num_pending_flushes_(0),
      max_num_vals_(0),
      max_num_data_entries_(0),
      rw_lock_(std::make_unique<ReadSkewedLock>()) {
  constexpr auto kMaxShardBytes =
      LL::value ? kLowLatencyMaxShardBytes : kBatchingMaxShardBytes;
  constexpr auto kMaxShardSize = kMaxShardBytes / sizeof(DataEntry);

  mapping_ = make_proclet<ShardMapping>(std::tuple(kMaxShardBytes, pinned_ip),
                                        pinned_ip.has_value(), std::nullopt,
                                        pinned_ip);

  std::vector<std::optional<Key>> keys;

  keys.push_back(std::nullopt);
  if (sharding_hint) {
    auto k = sharding_hint->estimated_min_key;
    auto num_shards = (sharding_hint->num - 1) / kMaxShardSize + 1;
    for (std::size_t i = 0; i < num_shards; i++) {
      keys.push_back(k);
      sharding_hint->key_inc_fn(k, kMaxShardSize);
    }
  }

  {
    std::vector<Future<WeakProclet<Shard>>> shard_futures;

    for (auto it = keys.begin(); it != keys.end(); it++) {
      auto curr_key = *it;
      auto next_key = (it + 1) == keys.end() ? std::optional<Key>() : *(it + 1);

      if constexpr (PushBackAble<Container>) {
        if (!curr_key) {
          shard_futures.emplace_back(mapping_.run_async(
              &ShardMapping::template create_new_shard<std::decay_t<As>...>,
              std::move(curr_key), std::optional<Key>(), args...));
          shard_futures.back().get();
        }
      } else {
        shard_futures.emplace_back(mapping_.run_async(
            &ShardMapping::template create_new_shard<std::decay_t<As>...>,
            std::move(curr_key), std::move(next_key), args...));
      }
    }
  }

  sync_mapping();
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline ShardedDataStructure<Container, LL>::ShardedDataStructure(
    const ShardedDataStructure &o)
    : mapping_(o.mapping_),
      last_mapping_seq_(o.last_mapping_seq_),
      mapping_seq_(o.mapping_seq_),
      key_to_shards_(o.key_to_shards_),
      num_pending_flushes_(0),
      max_num_vals_(o.max_num_vals_),
      max_num_data_entries_(o.max_num_data_entries_),
      rw_lock_(std::make_unique<ReadSkewedLock>()) {
  mapping_.run(&GeneralShardMapping<Shard>::client_register, mapping_seq_);
}

template <GeneralContainerBased Container, BoolIntegral LL>
ShardedDataStructure<Container, LL> &
ShardedDataStructure<Container, LL>::operator=(const ShardedDataStructure &o) {
  reset();
  const_cast<ShardedDataStructure &>(o).flush();

  mapping_ = o.mapping_;
  last_mapping_seq_ = o.last_mapping_seq_;
  mapping_seq_ = o.mapping_seq_;
  key_to_shards_ = o.key_to_shards_;
  num_pending_flushes_ = 0;
  max_num_vals_ = o.max_num_vals_;
  max_num_data_entries_ = o.max_num_data_entries_;

  mapping_.run(&GeneralShardMapping<Shard>::client_register, mapping_seq_);

  return *this;
}

template <GeneralContainerBased Container, BoolIntegral LL>
ShardedDataStructure<Container, LL>::ShardedDataStructure(
    ShardedDataStructure &&o) noexcept
    : mapping_(std::move(o.mapping_)),
      last_mapping_seq_(o.last_mapping_seq_),
      mapping_seq_(o.mapping_seq_),
      key_to_shards_(std::move(o.key_to_shards_)),
      push_back_reqs_(std::move(o.push_back_reqs_)),
      pending_flushes_links_(std::move(o.pending_flushes_links_)),
      num_pending_flushes_(o.num_pending_flushes_),
      max_num_vals_(o.max_num_vals_),
      max_num_data_entries_(o.max_num_data_entries_),
      rw_lock_(std::make_unique<ReadSkewedLock>()) {}

template <GeneralContainerBased Container, BoolIntegral LL>
ShardedDataStructure<Container, LL> &
ShardedDataStructure<Container, LL>::operator=(
    ShardedDataStructure &&o) noexcept {
  reset();

  mapping_ = std::move(o.mapping_);
  last_mapping_seq_ = o.last_mapping_seq_;
  mapping_seq_ = o.mapping_seq_;
  key_to_shards_ = std::move(o.key_to_shards_);
  push_back_reqs_ = std::move(o.push_back_reqs_);
  pending_flushes_links_ = std::move(o.pending_flushes_links_);
  num_pending_flushes_ = o.num_pending_flushes_;
  max_num_vals_ = o.max_num_vals_;
  max_num_data_entries_ = o.max_num_data_entries_;

  return *this;
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::reset() {
  if (mapping_) {
    flush();
    key_to_shards_.clear();
    mapping_.run(&GeneralShardMapping<Shard>::client_unregister, mapping_seq_);
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
ShardedDataStructure<Container, LL>::~ShardedDataStructure() {
  reset();
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::update_max_num_data_entries(
    KeyToShardsMapping::iterator iter) {
  max_num_data_entries_ = kBatchingMaxBatchBytes /
                          cereal::get_size(iter->second.insert_reqs.back());
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::update_max_num_vals() {
  max_num_vals_ =
      kBatchingMaxBatchBytes / cereal::get_size(push_back_reqs_.back());
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename K, typename V>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::insert(
    K &&k, V &&v)
  requires(HasVal<Container> && InsertAble<Container>)
{
  insert({std::forward<K>(k), std::forward<V>(v)});
}

template <GeneralContainerBased Container, BoolIntegral LL>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::insert(
    const DataEntry &entry)
  requires InsertAble<Container>
{
  __insert(const_cast<DataEntry &>(entry));
}

template <GeneralContainerBased Container, BoolIntegral LL>
[[gnu::always_inline]] inline void ShardedDataStructure<Container, LL>::insert(
    DataEntry &&entry)
  requires InsertAble<Container>
{
  __insert(std::move(entry));
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <bool Flush, typename D>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::__insert(D &&entry)
  requires InsertAble<Container>
{
[[maybe_unused]] retry:
  const Key *k_ptr;
  if constexpr (HasVal<Container>) {
    k_ptr = &entry.first;
  } else {
    k_ptr = &entry;
  }

  if constexpr (LL::value) {
    rw_lock_->reader_lock();
    auto iter = --key_to_shards_.upper_bound(*k_ptr);
    auto shard = iter->second.shard;
    rw_lock_->reader_unlock();
    auto succeed = shard.run(&Shard::try_insert, entry);

    if (unlikely(!succeed)) {
      sync_mapping();
      goto retry;
    }
  } else {
    auto iter = --key_to_shards_.upper_bound(*k_ptr);
    auto &reqs = iter->second.insert_reqs;
    if (unlikely(reqs.empty())) {
      reqs.reserve(max_num_data_entries_);
    }
    reqs.emplace_back(std::forward<D>(entry));

    if constexpr (Flush) {
      if (unlikely(reqs.size() >= max_num_data_entries_)) {
        update_max_num_data_entries(iter);
        flush_one_batch(iter, /* drain = */ false);
      }
    }
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
[[gnu::always_inline]] inline bool ShardedDataStructure<Container, LL>::erase(
    const Key &k)
  requires EraseAble<Container>
{
  return __erase(const_cast<Key &>(k));
}

template <GeneralContainerBased Container, BoolIntegral LL>
[[gnu::always_inline]] inline bool ShardedDataStructure<Container, LL>::erase(
    Key &&k)
  requires EraseAble<Container>
{
  return __erase(std::move(k));
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename K>
[[gnu::always_inline]] inline bool ShardedDataStructure<Container, LL>::__erase(
    K &&k)
  requires EraseAble<Container>
{
[[maybe_unused]] retry:
  rw_lock_->reader_lock();
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.shard;
  rw_lock_->reader_unlock();
  auto optional_erased = shard.run(&Shard::try_erase, k);

  if (unlikely(!optional_erased)) {
    sync_mapping();
    goto retry;
  }

  return *optional_erased;
}

template <GeneralContainerBased Container, BoolIntegral LL>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::push_back(const Val &v)
  requires PushBackAble<Container>
{
  __push_back(const_cast<Val &>(v));
}

template <GeneralContainerBased Container, BoolIntegral LL>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::push_back(Val &&v)
  requires PushBackAble<Container>
{
  __push_back(std::move(v));
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <bool Flush, typename V>
[[gnu::always_inline]] inline void
ShardedDataStructure<Container, LL>::__push_back(V &&v)
  requires PushBackAble<Container>
{
[[maybe_unused]] retry:
  if constexpr (LL::value) {
    run_at_border<false, void>(&Shard::try_push_back, std::forward<V>(v));
  } else {
    push_back_reqs_.emplace_back(std::forward<V>(v));

    if constexpr (Flush) {
      if (unlikely(push_back_reqs_.size() >= max_num_vals_)) {
        update_max_num_vals();
        auto iter = --key_to_shards_.end();
        flush_one_batch(iter, /* drain = */ false);
      }
    }
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <bool Front, typename RetT, typename F, typename... As>
RetT ShardedDataStructure<Container, LL>::run_at_border(F f, As &&...args) {
retry:
  std::optional<Key> l_key, r_key;
  WeakProclet<Shard> shard;

  rw_lock_->reader_lock();
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
  rw_lock_->reader_unlock();

  auto val = shard.run(f, l_key, r_key, args...);
  if (unlikely(!val)) {
    sync_mapping();
    goto retry;
  }

  if constexpr (!std::is_same_v<RetT, void>) {
    return *val;
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline Container::Val ShardedDataStructure<Container, LL>::front() const
  requires HasFront<Container>
{
  return const_cast<ShardedDataStructure *>(this)->__front();
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline Container::Val ShardedDataStructure<Container, LL>::__front()
  requires HasFront<Container>
{
  return run_at_border<true, Val>(&Shard::try_front);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline void ShardedDataStructure<Container, LL>::push_front(const Val &v)
  requires PushFrontAble<Container>
{
  __push_front(const_cast<Val &>(v));
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline void ShardedDataStructure<Container, LL>::push_front(Val &&v)
  requires PushFrontAble<Container>
{
  __push_front(std::move(v));
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename V>
inline void ShardedDataStructure<Container, LL>::__push_front(V &&v)
  requires PushFrontAble<Container>
{
[[maybe_unused]] retry:
  if constexpr (LL::value) {
    run_at_border<true, void>(&Shard::try_push_front, std::forward<V>(v));
  } else {
    // TODO: implement batching.
    BUG();
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline Container::Val ShardedDataStructure<Container, LL>::pop_front()
  requires TryPopFrontAble<Container>
{
  return run_at_border<true, Val>(&Shard::try_pop_front);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline std::vector<typename Container::Val>
ShardedDataStructure<Container, LL>::try_pop_front(std::size_t num)
  requires TryPopFrontAble<Container>
{
  return run_at_border<true, std::vector<Val>>(&Shard::try_pop_front_nb, num);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline Container::Val ShardedDataStructure<Container, LL>::back() const
  requires HasBack<Container>
{
  return const_cast<ShardedDataStructure *>(this)->__back();
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline Container::Val ShardedDataStructure<Container, LL>::__back()
  requires HasBack<Container>
{
  return run_at_border<false, Val>(&Shard::try_back);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline Container::Val ShardedDataStructure<Container, LL>::pop_back()
  requires TryPopBackAble<Container>
{
  return run_at_border<false, Val>(&Shard::try_pop_back);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline std::vector<typename Container::Val>
ShardedDataStructure<Container, LL>::try_pop_back(std::size_t num)
  requires TryPopBackAble<Container>
{
  return run_at_border<true, std::vector<Val>>(&Shard::try_pop_back_nb, num);
}

template <GeneralContainerBased Container, BoolIntegral LL>
std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::__find_data(Key k)
  requires FindDataAble<Container>
{
  flush();

retry:
  rw_lock_->reader_lock();
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.shard;
  rw_lock_->reader_unlock();
  auto [succeed, val] = shard.run(&Shard::find_data, k);

  if (unlikely(!succeed)) {
    sync_mapping();
    goto retry;
  }

  return val;
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline std::optional<typename ShardedDataStructure<Container, LL>::IterVal>
ShardedDataStructure<Container, LL>::find_data(Key k) const
  requires FindDataAble<Container>
{
  return const_cast<ShardedDataStructure *>(this)->__find_data(k);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline void ShardedDataStructure<Container, LL>::concat(
    ShardedDataStructure &&tail)
  requires Container::kContiguousIterator
{
  flush();
  tail.flush();
  mapping_.run_async(&ShardMapping::concat, tail.mapping_.get_weak());
  tail.key_to_shards_.clear();
}

template <GeneralContainerBased Container, BoolIntegral LL>
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

template <GeneralContainerBased Container, BoolIntegral LL>
inline ShardedDataStructure<Container, LL>::ShardAndReqs::ShardAndReqs(
    WeakProclet<Shard> s)
    : shard(s), seq(0) {}

template <GeneralContainerBased Container, BoolIntegral LL>
inline ShardedDataStructure<Container, LL>::ShardAndReqs::ShardAndReqs(
    const ShardAndReqs &o)
    : shard(o.shard), seq(0) {}

template <GeneralContainerBased Container, BoolIntegral LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::ShardAndReqs::save(
    Archive &ar) const {
  ar(shard);
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::ShardAndReqs::load(
    Archive &ar) {
  ar(shard);
  seq = 0;
}

template <GeneralContainerBased Container, BoolIntegral LL>
bool ShardedDataStructure<Container, LL>::pop_flush_future(
    std::deque<Future<std::optional<ReqBatch>>> *flush_futures,
    std::vector<ReqBatch> *rejected_batches) {
  bool rejected = false;

  auto &optional_batch = flush_futures->front().get();
  if (optional_batch) {
    rejected = true;
    rejected_batches->emplace_back(std::move(*optional_batch));
  }

  flush_futures->pop_front();
  num_pending_flushes_--;

  return rejected;
}

template <GeneralContainerBased Container, BoolIntegral LL>
std::vector<typename GeneralShard<Container>::ReqBatch>
ShardedDataStructure<Container, LL>::wait_for_pending_flushes(bool drain) {
  BUG_ON(num_pending_flushes_ > kMaxNumInflightFlushes + 1);
  std::vector<ReqBatch> rejected_batches;

  if (num_pending_flushes_ == kMaxNumInflightFlushes + 1 ||
      (drain && num_pending_flushes_)) {
    bool popped = false;

  again:
    for (auto it = pending_flushes_links_.begin();
         it != pending_flushes_links_.end();) {
      auto &flush_futures = (*it)->flush_futures;
      BUG_ON(flush_futures.empty());

      auto &front = flush_futures.front();
      if (drain || front.is_ready()) {
        popped = true;
        bool rejected = pop_flush_future(&flush_futures, &rejected_batches);

        if (unlikely(drain || rejected)) {
          while (!flush_futures.empty()) {
            pop_flush_future(&flush_futures, &rejected_batches);
          }
        }
        if (flush_futures.empty()) {
          it = pending_flushes_links_.erase(it);
          continue;
        }
      }
      it++;
    }

    if (unlikely(!popped)) {
      Time::sleep(kFlushFutureRecheckUs, /* high_priority = */ true);
      goto again;
    }
  }

  return rejected_batches;
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename RetT, typename... S0s, typename... S1s>
inline RetT ShardedDataStructure<Container, LL>::compute_on(
    Key k, RetT (*fn)(ContainerImpl &container, S0s...), S1s &&...states) {
[[maybe_unused]] retry:
  rw_lock_->reader_lock();
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.shard;
  rw_lock_->reader_unlock();
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto optional_ret =
      shard.template run</* MigrEn = */ true, /* CPUMon = */ true,
                         /* CPUSamp = */ false>(
          &Shard::template try_compute_on<RetT, S0s...>, k, fn_addr, states...);

  if (unlikely(!optional_ret)) {
    sync_mapping();
    goto retry;
  }

  if constexpr (!std::is_void_v<RetT>) {
    return *optional_ret;
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename RetT, typename... S0s, typename... S1s>
inline RetT ShardedDataStructure<Container, LL>::run(
    Key k, RetT (*fn)(ContainerImpl &container, Key k, S0s...),
    S1s &&...states) {
[[maybe_unused]] retry:
  rw_lock_->reader_lock();
  auto iter = --key_to_shards_.upper_bound(k);
  auto shard = iter->second.shard;
  rw_lock_->reader_unlock();
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto optional_ret =
      shard.run(&Shard::template try_run<RetT, S0s...>, k, fn_addr, states...);

  if (unlikely(!optional_ret)) {
    sync_mapping();
    goto retry;
  }

  if constexpr (!std::is_void_v<RetT>) {
    return *optional_ret;
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
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

  if (!batch.empty()) {
    if (unlikely(!shard_and_reqs.flush_executor)) {
      shard_and_reqs.flush_executor = shard_and_reqs.shard.run(+[](Shard &s) {
        return make_rem_unique<RobExecutor<ReqBatch, std::optional<ReqBatch>>>(
            [&](ReqBatch &&batch) { return s.try_handle_batch(batch); },
            kMaxNumInflightFlushes + 1);
      });
    }

    shard_and_reqs.flush_futures.emplace_back(
        shard_and_reqs.flush_executor.run_async(
            +[](RobExecutor<ReqBatch, std::optional<ReqBatch>> &rob_executor,
                uint32_t rob_seq, ReqBatch batch) {
              return rob_executor.submit(rob_seq, std::move(batch));
            },
            shard_and_reqs.seq++, std::move(batch)));
    num_pending_flushes_++;
    pending_flushes_links_.insert(&shard_and_reqs);
  }

  auto rejected_batches = wait_for_pending_flushes(drain);

  if (!rejected_batches.empty()) {
    handle_rejected_flush_batches(std::move(rejected_batches));
    return false;
  } else {
    return true;
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::reroute_reqs(
    std::vector<DataEntry> insert_reqs, std::vector<Val> push_back_reqs) {
  if constexpr (InsertAble<Container>) {
    for (auto &req : insert_reqs) {
      __insert</* Flush = */ false>(std::move(req));
    }
  }
  if constexpr (PushBackAble<Container>) {
    for (auto &req : push_back_reqs) {
      __push_back</* Flush = */ false>(std::move(req));
    }
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::handle_rejected_flush_batches(
    std::vector<ReqBatch> batches) {
  auto [last_insert_reqs, last_push_back_reqs] =
      sync_mapping(/* dont_reroute = */ true);
  for (auto &batch : batches) {
    reroute_reqs(std::move(batch.insert_reqs), std::move(batch.push_back_reqs));
  }
  reroute_reqs(std::move(last_insert_reqs), std::move(last_push_back_reqs));
}

template <GeneralContainerBased Container, BoolIntegral LL>
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

#ifdef DEBUG
  BUG_ON(num_pending_flushes_);
  BUG_ON(!pending_flushes_links_.empty());
  for (auto iter = key_to_shards_.begin(); iter != key_to_shards_.end();
       iter++) {
    BUG_ON(!iter->second.insert_reqs.empty());
    BUG_ON(!iter->second.flush_futures.empty());
  }
#endif
}

template <GeneralContainerBased Container, BoolIntegral LL>
std::pair<std::vector<typename ShardedDataStructure<Container, LL>::DataEntry>,
          std::vector<typename ShardedDataStructure<Container, LL>::Val>>
ShardedDataStructure<Container, LL>::sync_mapping(bool dont_reroute) {
  std::vector<DataEntry> insert_reqs;
  std::vector<Val> push_back_reqs;
  uint64_t old_seq = mapping_seq_;

  rw_lock_->writer_lock();
  if (old_seq == mapping_seq_) {
    auto v = mapping_.run(&ShardMapping::get_updates, mapping_seq_);
    typename ShardMapping::LogUpdates *updates;
    if (likely(updates = std::get_if<typename ShardMapping::LogUpdates>(&v))) {
      for (auto &entry : *updates) {
        if (entry.op == LogEntry<Shard>::kInsert) {
          auto it = key_to_shards_.emplace(entry.l_key, entry.shard);
          if (it != key_to_shards_.begin()) {
            move_append_vector(insert_reqs, (--it)->second.insert_reqs);
          }
        } else if (entry.op == LogEntry<Shard>::kMergeLeft ||
                   entry.op == LogEntry<Shard>::kMergeRight) {
          auto [begin_it, end_it] = key_to_shards_.equal_range(entry.l_key);
          auto it = begin_it;
          for (; it != end_it; ++it) {
            if (it->second.shard == entry.shard) {
              break;
            }
          }
          BUG_ON(it == end_it);

          move_append_vector(insert_reqs, it->second.insert_reqs);
          if (entry.op == LogEntry<Shard>::kMergeLeft) {
            it->second.flush_executor.release();
            key_to_shards_.erase(it);
          } else {
            auto next_it = std::next(it);
            BUG_ON(next_it == key_to_shards_.end());
            it->second = std::move(next_it->second.shard);
            next_it->second.flush_executor.release();
            key_to_shards_.erase(next_it);
          }
        } else {
          BUG();
        }
      }

      if (!updates->empty()) {
        BUG_ON(mapping_seq_ >= updates->back().seq);
        mapping_seq_ = updates->back().seq;
      }
    } else {
      auto rejected_batches = wait_for_pending_flushes(/* drain = */ true);
      for (auto &batch : rejected_batches) {
        move_append_vector(insert_reqs, batch.insert_reqs);
        move_append_vector(push_back_reqs, batch.push_back_reqs);
      }

      auto &snapshot = std::get<typename ShardMapping::Snapshot>(v);
      for (auto &[k, s] : key_to_shards_) {
        move_append_vector(insert_reqs, s.insert_reqs);
      }

      auto old_key_to_shards = std::move(key_to_shards_);
      key_to_shards_.clear();
      for (auto &[k, s] : snapshot.second) {
        auto new_it = key_to_shards_.emplace(k, std::move(s));
        auto [old_it_begin, old_it_end] = old_key_to_shards.equal_range(k);
        for (auto old_it = old_it_begin; old_it != old_it_end; ++old_it) {
          if (new_it->second.shard == old_it->second.shard) {
            new_it->second.seq = old_it->second.seq;
            new_it->second.flush_executor =
                std::move(old_it->second.flush_executor);
            break;
          }
        }
      }

      for (auto &[_, s] : old_key_to_shards) {
        s.flush_executor.release();
      }

      BUG_ON(mapping_seq_ >= snapshot.first);
      mapping_seq_ = snapshot.first;
    }
  } else {
    BUG_ON(old_seq > mapping_seq_);
  }
  rw_lock_->writer_unlock();

  move_append_vector(push_back_reqs, push_back_reqs_);
  push_back_reqs_.clear();

  if (!dont_reroute) {
    reroute_reqs(std::move(insert_reqs), std::move(push_back_reqs));
    return std::make_pair(std::vector<DataEntry>(), std::vector<Val>());
  } else {
    return std::make_pair(std::move(insert_reqs), std::move(push_back_reqs));
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::flush_and_sync_mapping() {
  flush();

  auto latest_mapping = mapping_.run(&ShardMapping::get_all_keys_and_shards);
  key_to_shards_.clear();
  for (auto &[k, s] : latest_mapping) {
    key_to_shards_.emplace(k, ShardAndReqs(s));
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename... S1s>
void ShardedDataStructure<Container, LL>::__for_all(auto *fn, S1s &&...states) {
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);

  for_all_shards(
      +[](ContainerImpl &container_impl, uintptr_t raw_fn, S1s... states) {
        auto *fn = reinterpret_cast<Fn>(raw_fn);
        container_impl.for_all(fn, states...);
      },
      raw_fn, std::forward<S1s>(states)...);
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all(void (*fn)(const Key &key,
                                                             Val &val, S0s...),
                                                  S1s &&...states)
  requires HasVal<Container>
{
  __for_all(fn, std::forward<S1s>(states)...);
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all(void (*fn)(const Key &key,
                                                             S0s...),
                                                  S1s &&...states)
  requires(!HasVal<Container>)
{
  __for_all(fn, std::forward<S1s>(states)...);
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <typename... S0s, typename... S1s>
void ShardedDataStructure<Container, LL>::for_all_shards(
    void (*fn)(ContainerImpl &container_impl, S0s...), S1s &&...states) {
  flush_and_sync_mapping();

  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);

  std::vector<Future<void>> futures;

  rw_lock_->reader_lock();
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    auto &shard = shard_and_reqs.shard;
    futures.emplace_back(shard.run_async(
        +[](Shard &shard, uintptr_t raw_fn, S0s... states) {
          auto *fn = reinterpret_cast<Fn>(raw_fn);
          auto container_ptr = shard.get_container_handle();
          container_ptr->pass_through(fn, states...);
        },
        raw_fn, states...));
  }
  rw_lock_->reader_unlock();
}

template <GeneralContainerBased Container, BoolIntegral LL>
Container ShardedDataStructure<Container, LL>::collect() {
  flush_and_sync_mapping();

  std::vector<Future<Container>> futures;
  rw_lock_->reader_lock();
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(
        shard_and_reqs.shard.template run_async</* MigrEn = */ false>(
            &Shard::get_container_copy));
  }
  rw_lock_->reader_unlock();

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

// Designed to be thread-safe.
template <GeneralContainerBased Container, BoolIntegral LL>
std::size_t ShardedDataStructure<Container, LL>::__size() {
  flush_and_sync_mapping();

  std::vector<Future<std::size_t>> futures;
  rw_lock_->reader_lock();
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(shard_and_reqs.shard.run_async(+[](Shard &s) {
      auto handle = s.get_container_handle();
      // The shard may be at the creation stage.
      return handle ? handle->size() : 0;
    }));
  }
  rw_lock_->reader_unlock();

  std::size_t size = 0;
  for (auto &future : futures) {
    size += future.get();
  }

  return size;
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline std::size_t ShardedDataStructure<Container, LL>::size() const {
  return const_cast<ShardedDataStructure *>(this)->__size();
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline bool ShardedDataStructure<Container, LL>::empty() const {
  return !size();
}

template <GeneralContainerBased Container, BoolIntegral LL>
void ShardedDataStructure<Container, LL>::clear()
  requires ClearAble<Container>
{
  flush_and_sync_mapping();

  std::vector<Future<void>> futures;
  rw_lock_->reader_lock();
  for (auto &[_, shard_and_reqs] : key_to_shards_) {
    futures.emplace_back(shard_and_reqs.shard.run_async(
        +[](Shard &s) { s.get_container_handle()->clear(); }));
  }
  rw_lock_->reader_unlock();

  for (auto &future : futures) {
    future.get();
  }
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::__save(Archive &ar) {
  flush();
  ar(mapping_, last_mapping_seq_, mapping_seq_, key_to_shards_, max_num_vals_,
     max_num_data_entries_);
  mapping_.run(&GeneralShardMapping<Shard>::client_register, mapping_seq_);
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::save(Archive &ar) const {
  const_cast<ShardedDataStructure *>(this)->__save(ar);
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::save_move(Archive &ar) {
  flush();
  ar(std::move(mapping_), last_mapping_seq_, mapping_seq_, key_to_shards_,
     max_num_vals_, max_num_data_entries_);
  key_to_shards_.clear();
}

template <GeneralContainerBased Container, BoolIntegral LL>
template <class Archive>
inline void ShardedDataStructure<Container, LL>::load(Archive &ar) {
  ar(mapping_, last_mapping_seq_, mapping_seq_, key_to_shards_, max_num_vals_,
     max_num_data_entries_);
}

template <GeneralContainerBased Container, BoolIntegral LL>
std::vector<std::tuple<
    std::optional<typename ShardedDataStructure<Container, LL>::Key>,
    std::size_t,
    WeakProclet<typename ShardedDataStructure<Container, LL>::Shard>>>
ShardedDataStructure<Container, LL>::get_all_shards_info() {
  flush_and_sync_mapping();

  std::vector<std::tuple<std::optional<Key>, std::size_t, WeakProclet<Shard>>>
      ret;
  std::vector<nu::Thread> ths;

  rw_lock_->reader_lock();
  ret.reserve(key_to_shards_.size());
  ths.reserve(key_to_shards_.size());
  for (auto &[k, shard_and_reqs] : key_to_shards_) {
    auto shard = shard_and_reqs.shard.get_weak();
    ret.emplace_back(k, 0, shard);
    auto *size_ptr = &std::get<1>(ret.back());
    ths.emplace_back(
        [size_ptr, shard]() mutable { *size_ptr = shard.run(&Shard::size); });
  }
  rw_lock_->reader_unlock();

  for (auto &th : ths) {
    th.join();
  }

  return ret;
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline void ShardedDataStructure<Container, LL>::seal() {
  mapping_.run(&ShardMapping::seal);
}

template <GeneralContainerBased Container, BoolIntegral LL>
inline void ShardedDataStructure<Container, LL>::unseal() {
  mapping_.run(&ShardMapping::unseal);
}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL>::BackInsertIterator(
    ShardedDataStructure<Container, LL> &ds)
    : ds_(ds) {}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL> &
BackInsertIterator<Container, LL>::operator++() {
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL> &
BackInsertIterator<Container, LL>::operator*() {
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
inline BackInsertIterator<Container, LL> &
BackInsertIterator<Container, LL>::operator=(const Val &val) {
  ds_.push_back(val);
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
inline BackInsertIterator<Container, LL> &
BackInsertIterator<Container, LL>::operator=(Val &&val) {
  ds_.push_back(std::move(val));
  return *this;
}

template <PushBackAble Container, BoolIntegral LL>
BackInsertIterator<Container, LL> back_inserter(
    ShardedDataStructure<Container, LL> &sharded_ds) {
  return BackInsertIterator(sharded_ds);
};

}  // namespace nu

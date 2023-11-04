#include "nu/utils/caladan.hpp"
#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/time.hpp"

namespace nu {

template <class Shard>
template <class Archive>
void LogEntry<Shard>::serialize(Archive &ar) {
  ar(op, l_key, shard, seq);
}

template <class Shard>
Log<Shard>::Log(uint32_t size) : seq_(0), cb_(size) {
  cb_.push_back();
  cb_.front().seq = 0;
}

template <class Shard>
std::optional<std::vector<LogEntry<Shard>>> Log<Shard>::from(
    uint64_t start_seq) {
  BUG_ON(start_seq > cb_.back().seq + 1);

  std::vector<LogEntry<Shard>> slice;

  auto it = cb_.begin();
  if (unlikely(it->seq > start_seq)) {
    // start_seq is too old.
    return std::nullopt;
  }
  if (unlikely(it == cb_.end() || start_seq == cb_.back().seq + 1)) {
    // No update.
    return slice;
  }

  it += start_seq - it->seq;
  std::copy(it, cb_.end(), std::back_inserter(slice));

  return slice;
}

template <class Shard>
uint64_t Log<Shard>::last_seq() const {
  return seq_;
}

template <class Shard>
void Log<Shard>::append(uint8_t op, std::optional<typename Shard::Key> l_key,
                        WeakProclet<Shard> shard) {
  cb_.push_back(LogEntry<Shard>{op, std::move(l_key), shard, ++seq_});
}

template <class Shard>
GeneralShardMapping<Shard>::GeneralShardMapping(uint32_t max_shard_bytes,
                                                std::optional<NodeIP> pinned_ip)
    : max_shard_bytes_(max_shard_bytes),
      proclet_capacity_(max_shard_bytes_ * kProcletOverprovisionFactor),
      pinned_ip_(pinned_ip),
      ref_cnt_(1),
      log_(kLogSize),
      last_gc_us_(0) {
  {
    Caladan::PreemptGuard g;
    self_ = get_runtime()->get_current_weak_proclet<GeneralShardMapping>();
  }

  client_seqs_.emplace(0);
}

template <class Shard>
GeneralShardMapping<Shard>::~GeneralShardMapping() {
  BUG_ON(ref_cnt_);
}

template <class Shard>
void GeneralShardMapping<Shard>::seal() {
  ScopedLock lock(&mutex_);

  BUG_ON(!ref_cnt_);  // Cannot be sealed twice.
  while (Caladan::access_once(ref_cnt_) != 1) {
    // Wait until there's only one ref.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_ = 0;
}

template <class Shard>
void GeneralShardMapping<Shard>::unseal() {
  ScopedLock lock(&mutex_);

  BUG_ON(ref_cnt_);
  ref_cnt_ = 1;
  ref_cnt_cv_.signal();  // unblock inc_ref_cnt().
}

template <class Shard>
void GeneralShardMapping<Shard>::client_register(uint64_t seq) {
  ScopedLock lock(&mutex_);

  client_seqs_.emplace(seq);

  while (!Caladan::access_once(ref_cnt_)) {
    // Wait until it is unsealed.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_++;
  BUG_ON(!ref_cnt_);
}

template <class Shard>
void GeneralShardMapping<Shard>::client_unregister(uint64_t seq) {
  ScopedLock lock(&mutex_);

  BUG_ON(!ref_cnt_);
  ref_cnt_--;
  ref_cnt_cv_.signal();  // unblock seal().

  auto iter = client_seqs_.find(seq);
  BUG_ON(iter == client_seqs_.end());
  client_seqs_.erase(iter);
}

template <class Shard>
std::variant<typename GeneralShardMapping<Shard>::LogUpdates,
             typename GeneralShardMapping<Shard>::Snapshot>
GeneralShardMapping<Shard>::get_updates(uint64_t client_seq) {
  ScopedLock lock(&mutex_);

  check_gc_locked();

  auto iter = client_seqs_.find(client_seq);
  BUG_ON(iter == client_seqs_.end());
  client_seqs_.erase(iter);
  client_seqs_.emplace(log_.last_seq());

  auto optional_log_updates = log_.from(client_seq + 1);
  if (likely(optional_log_updates)) {
    return *optional_log_updates;
  } else {
    return get_snapshot(lock);
  }
}

template <class Shard>
typename GeneralShardMapping<Shard>::Snapshot
GeneralShardMapping<Shard>::get_snapshot(const ScopedLock<Mutex> &lock) {
  Snapshot snapshot;

  snapshot.first = log_.last_seq();
  for (auto &[k, sl] : mapping_) {
    snapshot.second.emplace(k, sl.shard.get_weak());
  }

  return snapshot;
}

template <class Shard>
std::vector<std::pair<std::optional<typename Shard::Key>, WeakProclet<Shard>>>
GeneralShardMapping<Shard>::get_all_keys_and_shards() {
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
      keys_and_shards;

  {
    ScopedLock lock(&mutex_);

    for (auto &[k, sl] : mapping_) {
      keys_and_shards.emplace_back(k, sl.shard.get_weak());
    }
  }

  return keys_and_shards;
}

template <class Shard>
WeakProclet<Shard> GeneralShardMapping<Shard>::get_shard_for_key(
    std::optional<Key> key) {
  ScopedLock lock(&mutex_);

  auto iter = --mapping_.upper_bound(key);
  auto shard = iter->second.shard.get_weak();
  return shard;
}

template <class Shard>
std::vector<Proclet<Shard>> GeneralShardMapping<Shard>::move_all_shards() {
  std::vector<Proclet<Shard>> shards;

  {
    ScopedLock lock(&mutex_);
    for (auto &[k, sl] : mapping_) {
      shards.emplace_back(std::move(sl.shard));
    }
  }

  return shards;
}

template <class Shard>
template <typename... As>
WeakProclet<Shard> GeneralShardMapping<Shard>::create_new_shard(
    std::optional<Key> l_key, std::optional<Key> r_key, As... args) {
  auto new_shard = make_proclet<Shard>(
      std::forward_as_tuple(self_, max_shard_bytes_, l_key, r_key,
                            std::move(args)...),
      pinned_ip_.has_value(), proclet_capacity_, pinned_ip_);
  auto new_weak_shard = new_shard.get_weak();

  {
    ScopedLock lock(&mutex_);

    log_.append(LogEntry<Shard>::kInsert, l_key, new_weak_shard);
    mapping_.emplace(l_key,
                     ShardWithLifetime{std::move(new_shard), log_.last_seq()});
  }
  return new_weak_shard;
}

template <class Shard>
std::pair<bool, WeakProclet<Shard>>
GeneralShardMapping<Shard>::create_or_reuse_new_shard_for_init(
    std::optional<Key> l_key, NodeIP ip) {
  Proclet<Shard> new_shard;
  bool reuse = false;

  {
    ScopedLock lock(&mutex_);

    // Try to reuse deleted shards, useful for services.
    auto &shards = shards_to_reuse_[ip];
    if (!shards.empty()) {
      new_shard = std::move(shards.top());
      shards.pop();
      reuse = true;
    }
  }

  if (!new_shard) {
    // Useful for improving the data locality.
    if (Shard::kIsService) {
      new_shard =
          make_proclet<Shard>(std::forward_as_tuple(self_, max_shard_bytes_),
                              pinned_ip_.has_value(), proclet_capacity_, ip);
    } else {
      new_shard = make_proclet<Shard>(
          std::forward_as_tuple(self_, max_shard_bytes_),
          pinned_ip_.has_value(), proclet_capacity_, pinned_ip_);
    }
  }

  auto new_weak_shard = new_shard.get_weak();
  {
    ScopedLock lock(&mutex_);
    log_.append(LogEntry<Shard>::kInsert, l_key, new_weak_shard);
    mapping_.emplace(std::move(l_key),
                     ShardWithLifetime{std::move(new_shard), log_.last_seq()});
  }

  return std::make_pair(reuse, new_weak_shard);
}

template <class Shard>
bool GeneralShardMapping<Shard>::delete_shard(std::optional<Key> l_key,
                                              WeakProclet<Shard> shard,
                                              bool merge_left, NodeIP ip,
                                              std::optional<float> cpu_load) {
  ScopedLock<Mutex> lock(&mutex_);

  check_gc_locked();

  auto [begin_it, end_it] = mapping_.equal_range(l_key);

  decltype(begin_it) it;
  for (it = begin_it; it != end_it; ++it) {
    if (it->second.shard == shard) {
      break;
    }
  }
  BUG_ON(it == end_it);

  auto prev_it = std::prev(it);
  auto next_it = std::next(it);
  if (merge_left) {
    BUG_ON(it == mapping_.begin());
    std::optional<Key> r_key =
        next_it != mapping_.end() ? next_it->first : std::nullopt;
    if (unlikely(!prev_it->second.shard.run(&Shard::try_merge,
                                            /* merge_left = */ false, r_key,
                                            cpu_load))) {
      return false;
    }
  } else {
    BUG_ON(next_it == mapping_.end());
    if (unlikely(!next_it->second.shard.run(&Shard::try_merge,
                                            /* merge_left = */ true, l_key,
                                            cpu_load))) {
      return false;
    }
    auto next_node = mapping_.extract(next_it);
    next_node.key() = l_key;
    mapping_.insert(std::move(next_node));
  }

  log_.append(
      merge_left ? LogEntry<Shard>::kMergeLeft : LogEntry<Shard>::kMergeRight,
      it->first, shard);
  it->second.end_seq = log_.last_seq();
  if constexpr (Shard::kIsService) {
    shards_to_reuse_[ip].emplace(std::move(it->second.shard));
  } else {
    shards_to_gc_.emplace_back(std::move(it->second));
  }
  mapping_.erase(it);
  return true;
}

template <class Shard>
void GeneralShardMapping<Shard>::concat(WeakProclet<GeneralShardMapping> tail)
  requires(Shard::GeneralContainer::kContiguousIterator)
{
  auto all_tail_shards = tail.run_async(&GeneralShardMapping::move_all_shards);
  auto end_key = (--mapping_.end())->second.shard.run(&Shard::split_at_end);

  for (auto &tail_shard : all_tail_shards.get()) {
    log_.append(LogEntry<Shard>::kInsert, end_key, tail_shard.get_weak());
    auto iter = mapping_.emplace(
        end_key, ShardWithLifetime{std::move(tail_shard), log_.last_seq()});
    end_key = iter->second.shard.run(&Shard::rebase, end_key);
  }
}

template <class Shard>
inline void GeneralShardMapping<Shard>::check_gc_locked() {
  std::vector<Proclet<Shard>> gc;

  auto cur_us = Time::microtime();
  if (cur_us < last_gc_us_ + kGCIntervalUs) {
    return;
  }

  last_gc_us_ = cur_us;
  for (auto it = shards_to_gc_.begin(); it != shards_to_gc_.end();) {
    auto client_iter = client_seqs_.lower_bound(it->end_seq);
    if (client_iter == client_seqs_.begin() ||
        *std::prev(client_iter) < it->start_seq) {
      gc.emplace_back(std::move(it->shard));
      it = shards_to_gc_.erase(it);
    } else {
      ++it;
    }
  }

  // Destruct shards asynchronously.
  if (!gc.empty()) {
    shard_destruction_ = nu::async([gc = std::move(gc)] {});
  }
}

}  // namespace nu

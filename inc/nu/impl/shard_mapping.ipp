#include "nu/utils/caladan.hpp"
#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <class Shard>
template <class Archive>
void LogEntry<Shard>::serialize(Archive &ar) {
  ar(op, l_key, shard, seq);
}

template <class Shard>
Log<Shard>::Log(uint32_t size) : seq_(0), cb_(size) {}

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
  return seq_ - 1;
}

template <class Shard>
void Log<Shard>::append(uint8_t op, std::optional<typename Shard::Key> l_key,
                        WeakProclet<Shard> shard) {
  cb_.push_back(LogEntry<Shard>{op, std::move(l_key), shard, seq_++});
}

template <class Shard>
GeneralShardMapping<Shard>::GeneralShardMapping(
    uint32_t max_shard_bytes, std::optional<uint32_t> max_shard_cnt)
    : max_shard_bytes_(max_shard_bytes),
      proclet_capacity_(max_shard_bytes_ * kProcletOverprovisionFactor),
      max_shard_cnt_(max_shard_cnt),
      pending_creations_(0),
      ref_cnt_(1),
      log_(kLogSize) {
  Caladan::PreemptGuard g;
  self_ = get_runtime()->get_current_weak_proclet<GeneralShardMapping>();
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
void GeneralShardMapping<Shard>::inc_ref_cnt() {
  ScopedLock lock(&mutex_);

  while (!Caladan::access_once(ref_cnt_)) {
    // Wait until it is unsealed.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_++;
  BUG_ON(!ref_cnt_);
}

template <class Shard>
void GeneralShardMapping<Shard>::dec_ref_cnt() {
  ScopedLock lock(&mutex_);

  BUG_ON(!ref_cnt_);
  ref_cnt_--;
  ref_cnt_cv_.signal();  // unblock seal().
}

template <class Shard>
std::variant<typename GeneralShardMapping<Shard>::LogUpdates,
             typename GeneralShardMapping<Shard>::Snapshot>
GeneralShardMapping<Shard>::get_updates(uint64_t start_seq) {
  ScopedLock lock(&mutex_);

  auto optional_log_updates = log_.from(start_seq);
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
  for (auto &[k, p] : mapping_) {
    snapshot.second.emplace(k, p.get_weak());
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

    for (auto &[k, s] : mapping_) {
      keys_and_shards.emplace_back(k, s.get_weak());
    }
  }

  return keys_and_shards;
}

template <class Shard>
WeakProclet<Shard> GeneralShardMapping<Shard>::get_shard_for_key(
    std::optional<Key> key) {
  ScopedLock lock(&mutex_);

  auto iter = --mapping_.upper_bound(key);
  auto shard = iter->second.get_weak();
  return shard;
}

template <class Shard>
std::vector<Proclet<Shard>> GeneralShardMapping<Shard>::move_all_shards() {
  std::vector<Proclet<Shard>> shards;

  {
    ScopedLock lock(&mutex_);
    for (auto &[k, s] : mapping_) {
      shards.emplace_back(std::move(s));
    }
  }

  return shards;
}

template <class Shard>
void GeneralShardMapping<Shard>::reserve_new_shard() {
  ScopedLock lock(&mutex_);

  auto new_shard = make_proclet<Shard>(std::tuple(self_, max_shard_bytes_),
                                       false, proclet_capacity_);
  reserved_shards_.emplace(std::move(new_shard));
}

template <class Shard>
WeakProclet<Shard> GeneralShardMapping<Shard>::create_new_shard(
    std::optional<Key> l_key, std::optional<Key> r_key) {
  {
    ScopedLock lock(&mutex_);

    while (out_of_shards()) {
      oos_cv_.wait(&mutex_);
    }
    pending_creations_++;
  }

  auto new_shard = make_proclet<Shard>(
      std::forward_as_tuple(self_, max_shard_bytes_, l_key, r_key), false,
      proclet_capacity_);
  auto new_weak_shard = new_shard.get_weak();

  {
    ScopedLock lock(&mutex_);

    log_.append(LogEntry<Shard>::kInsert, l_key, new_weak_shard);
    mapping_.emplace(l_key, std::move(new_shard));
    pending_creations_--;
  }
  return new_weak_shard;
}

template <class Shard>
WeakProclet<Shard>
GeneralShardMapping<Shard>::create_or_reuse_new_shard_for_init(
    std::optional<Key> l_key, NodeIP ip) {
  {
    ScopedLock lock(&mutex_);

    while (out_of_shards()) {
      oos_cv_.wait(&mutex_);
    }
    pending_creations_++;
  }

  Proclet<Shard> new_shard;

  // Useful for queue & stack.
  if (!reserved_shards_.empty()) {
    ScopedLock lock(&mutex_);

    if (likely(!reserved_shards_.empty())) {
      new_shard = std::move(reserved_shards_.top());
      reserved_shards_.pop();
    }
  }

  if (!new_shard) {
    // Useful for improving the locality of sorter.
    if (mapping_.size() >= kCreateLocalShardThresh) {
      new_shard =
          make_proclet<Shard>(std::forward_as_tuple(self_, max_shard_bytes_),
                              false, proclet_capacity_, ip);
    } else {
      new_shard =
          make_proclet<Shard>(std::forward_as_tuple(self_, max_shard_bytes_),
                              false, proclet_capacity_);
    }
  }

  auto new_weak_shard = new_shard.get_weak();
  {
    ScopedLock lock(&mutex_);
    auto ret = stash_mapping_.emplace(std::move(l_key), std::move(new_shard));
    BUG_ON(!ret.second);
  }

  return new_weak_shard;
}

template <class Shard>
void GeneralShardMapping<Shard>::commit_shard(std::optional<Key> l_key) {
  ScopedLock lock(&mutex_);

  auto iter = stash_mapping_.find(l_key);
  BUG_ON(iter == stash_mapping_.end());

  log_.append(LogEntry<Shard>::kInsert, l_key, iter->second.get_weak());
  mapping_.emplace(std::move(l_key), std::move(iter->second));
  pending_creations_--;

  stash_mapping_.erase(iter);
}

template <class Shard>
void GeneralShardMapping<Shard>::delete_shard(std::optional<Key> l_key,
                                              WeakProclet<Shard> shard) {
  ScopedLock<Mutex> lock(&mutex_);

  auto [begin_it, end_it] = mapping_.equal_range(l_key);
  for (auto it = begin_it; it != end_it; ++it) {
    if (it->second == shard) {
      log_.append(LogEntry<Shard>::kDelete, it->first, shard);
      reserved_shards_.emplace(std::move(it->second));
      mapping_.erase(it);
      oos_cv_.signal();
      return;
    }
  }
  BUG();
}

template <class Shard>
void GeneralShardMapping<Shard>::concat(
    WeakProclet<GeneralShardMapping>
        tail) requires(Shard::GeneralContainer::kContiguousIterator) {
  auto all_tail_shards = tail.run_async(&GeneralShardMapping::move_all_shards);
  auto end_key = (--mapping_.end())->second.run(&Shard::split_at_end);

  for (auto &tail_shard : all_tail_shards.get()) {
    log_.append(LogEntry<Shard>::kInsert, end_key, tail_shard.get_weak());
    auto iter = mapping_.emplace(end_key, std::move(tail_shard));
    end_key = iter->second.run(&Shard::rebase, end_key);
  }
}

template <class Shard>
inline bool GeneralShardMapping<Shard>::out_of_shards() {
  if (max_shard_cnt_) {
    auto curr_cnt = mapping_.size() + pending_creations_;
    BUG_ON(curr_cnt > *max_shard_cnt_);
    return curr_cnt == *max_shard_cnt_;
  }

  return false;
}

}  // namespace nu

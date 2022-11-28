#include "nu/utils/caladan.hpp"

namespace nu {

template <class Shard>
GeneralShardMapping<Shard>::GeneralShardMapping(
    uint32_t max_shard_bytes, std::optional<uint32_t> max_shard_count)
    : max_shard_bytes_(max_shard_bytes),
      proclet_capacity_(max_shard_bytes_ * kProcletOverprovisionFactor),
      max_shard_count_(max_shard_count),
      ref_cnt_(1) {
  Caladan::PreemptGuard g;
  self_ = get_runtime()->get_current_weak_proclet<GeneralShardMapping>();
}

template <class Shard>
GeneralShardMapping<Shard>::~GeneralShardMapping() {
  BUG_ON(ref_cnt_);
}

template <class Shard>
void GeneralShardMapping<Shard>::seal() {
  mutex_.lock();
  BUG_ON(!ref_cnt_);  // Cannot be sealed twice.
  while (Caladan::access_once(ref_cnt_) != 1) {
    // Wait until there's only one ref.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_ = 0;
  mutex_.unlock();
}

template <class Shard>
void GeneralShardMapping<Shard>::unseal() {
  mutex_.lock();
  BUG_ON(ref_cnt_);
  ref_cnt_ = 1;
  mutex_.unlock();
  ref_cnt_cv_.signal();  // unblock inc_ref_cnt().
}

template <class Shard>
void GeneralShardMapping<Shard>::inc_ref_cnt() {
  mutex_.lock();
  while (!Caladan::access_once(ref_cnt_)) {
    // Wait until it is unsealed.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_++;
  BUG_ON(!ref_cnt_);
  mutex_.unlock();
}

template <class Shard>
void GeneralShardMapping<Shard>::dec_ref_cnt() {
  mutex_.lock();
  BUG_ON(!ref_cnt_);
  ref_cnt_--;
  mutex_.unlock();
  ref_cnt_cv_.signal();  // unblock seal().
}

template <class Shard>
std::vector<std::pair<std::optional<typename Shard::Key>, WeakProclet<Shard>>>
GeneralShardMapping<Shard>::get_shards_in_range(std::optional<Key> l_key,
                                                std::optional<Key> r_key) {
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>> shards;
  bool normal_range = (l_key != r_key);

  mutex_.lock();
  for (auto iter = mapping_.lower_bound(l_key); iter != mapping_.end();
       ++iter) {
    bool in_range =
        r_key ? (normal_range ? iter->first < r_key : iter->first <= r_key)
              : true;
    if (!in_range) {
      break;
    }

    shards.emplace_back(iter->first, iter->second.get_weak());
  }
  mutex_.unlock();

  return shards;
}

template <class Shard>
std::vector<std::pair<std::optional<typename Shard::Key>, WeakProclet<Shard>>>
GeneralShardMapping<Shard>::get_all_keys_and_shards() {
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
      keys_and_shards;

  mutex_.lock();
  for (auto &[k, s] : mapping_) {
    keys_and_shards.emplace_back(k, s.get_weak());
  }
  mutex_.unlock();

  return keys_and_shards;
}

template <class Shard>
std::optional<WeakProclet<Shard>> GeneralShardMapping<Shard>::get_shard_for_key(
    std::optional<Key> key) {
  mutex_.lock();
  auto iter = --mapping_.upper_bound(key);
  auto shard = iter->second.get_weak();
  mutex_.unlock();
  return shard;
}

template <class Shard>
std::vector<Proclet<Shard>> GeneralShardMapping<Shard>::acquire_all_shards() {
  std::vector<Proclet<Shard>> shards;

  mutex_.lock();
  for (auto &[k, s] : mapping_) {
    shards.emplace_back(std::move(s));
  }
  mutex_.unlock();

  return shards;
}

template <class Shard>
void GeneralShardMapping<Shard>::reserve_new_shard() {
  mutex_.lock();
  auto new_shard = make_proclet_with_capacity<Shard>(proclet_capacity_, self_,
                                                     max_shard_bytes_);
  reserved_shards_.emplace(std::move(new_shard));
  mutex_.unlock();
}

template <class Shard>
std::optional<WeakProclet<Shard>> GeneralShardMapping<Shard>::create_new_shard(
    std::optional<Key> l_key, std::optional<Key> r_key, bool reserve_space) {
  if (reached_size_bound()) {
    return std::nullopt;
  }

  std::optional<Proclet<Shard>> new_shard;

  if (!reserved_shards_.empty()) {
    mutex_.lock();
    if (unlikely(reached_size_bound())) {
      mutex_.unlock();
      return std::nullopt;
    }
    if (likely(!reserved_shards_.empty())) {
      new_shard = std::make_optional(std::move(reserved_shards_.top()));
      reserved_shards_.pop();
      mutex_.unlock();

      ContainerAndMetadata<typename Shard::GeneralContainer> data;
      (*new_shard).run(&Shard::set_range_and_data, l_key, r_key, data);
    } else {
      mutex_.unlock();
    }
  }

  if (!new_shard) {
    new_shard = std::make_optional(make_proclet_with_capacity<Shard>(
        proclet_capacity_, self_, max_shard_bytes_, l_key, r_key,
        reserve_space));
  }

  auto new_weak_shard = (*new_shard).get_weak();

  mutex_.lock();
  if (unlikely(reached_size_bound())) {
    mutex_.unlock();
    return std::nullopt;
  }
  mapping_.emplace(l_key, std::move(*new_shard));
  mutex_.unlock();

  return new_weak_shard;
}

template <class Shard>
bool GeneralShardMapping<Shard>::delete_front_shard() {
  ScopedLock<Mutex> guard(&mutex_);
  auto it = mapping_.begin();
  if (unlikely(it == mapping_.end())) {
    return false;
  }
  if (unlikely(it == --mapping_.end())) {
    return false;  // keep the tail shard alive
  }
  reserved_shards_.emplace(std::move(it->second));
  mapping_.erase(it);

  return true;
}

template <class Shard>
void GeneralShardMapping<Shard>::concat(
    WeakProclet<GeneralShardMapping>
        tail) requires(Shard::GeneralContainer::kContiguousIterator) {
  auto all_tail_shards =
      tail.run_async(&GeneralShardMapping::acquire_all_shards);
  auto end_key = (--mapping_.end())->second.run(&Shard::split_at_end);

  for (auto &tail_shard : all_tail_shards.get()) {
    auto iter = mapping_.emplace(end_key, std::move(tail_shard));
    end_key = iter->second.run(&Shard::rebase, end_key);
  }
}

template <class Shard>
inline bool GeneralShardMapping<Shard>::reached_size_bound() {
  return (max_shard_count_.has_value() && mapping_.size() >= *max_shard_count_);
}

}  // namespace nu

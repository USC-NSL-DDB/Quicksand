#include "nu/utils/caladan.hpp"

namespace nu {

template <class Shard>
GeneralShardingMapping<Shard>::GeneralShardingMapping(uint32_t max_shard_bytes)
    : max_shard_bytes_(max_shard_bytes),
      proclet_capacity_(max_shard_bytes_ * kProcletOverprovisionFactor),
      ref_cnt_(1) {
  Caladan::PreemptGuard g;
  self_ = get_runtime()->get_current_weak_proclet<GeneralShardingMapping>();
}

template <class Shard>
GeneralShardingMapping<Shard>::~GeneralShardingMapping() {
  BUG_ON(ref_cnt_);
}

template <class Shard>
void GeneralShardingMapping<Shard>::seal() {
  mutex_.lock();
  BUG_ON(!ref_cnt_);                        // Cannot be sealed twice.
  while (Caladan::access_once(ref_cnt_) !=
         1) {  // Wait until there's only one ref.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_ = 0;
  mutex_.unlock();
}

template <class Shard>
void GeneralShardingMapping<Shard>::unseal() {
  mutex_.lock();
  BUG_ON(ref_cnt_);
  ref_cnt_ = 1;
  mutex_.unlock();
  ref_cnt_cv_.signal();  // unblock inc_ref_cnt().
}

template <class Shard>
void GeneralShardingMapping<Shard>::inc_ref_cnt() {
  mutex_.lock();
  while (!Caladan::access_once(ref_cnt_)) {  // Wait until it is unsealed.
    ref_cnt_cv_.wait(&mutex_);
  }
  ref_cnt_++;
  BUG_ON(!ref_cnt_);
  mutex_.unlock();
}

template <class Shard>
void GeneralShardingMapping<Shard>::dec_ref_cnt() {
  mutex_.lock();
  BUG_ON(!ref_cnt_);
  ref_cnt_--;
  mutex_.unlock();
  ref_cnt_cv_.signal();  // unblock seal().
}

template <class Shard>
std::vector<std::pair<std::optional<typename Shard::Key>, WeakProclet<Shard>>>
GeneralShardingMapping<Shard>::get_shards_in_range(std::optional<Key> l_key,
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
GeneralShardingMapping<Shard>::get_all_shards() {
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>> shards;

  mutex_.lock();
  for (auto &[k, s] : mapping_) {
    shards.emplace_back(k, s.get_weak());
  }
  mutex_.unlock();

  return shards;
}

template <class Shard>
std::optional<WeakProclet<Shard>>
GeneralShardingMapping<Shard>::get_shard_for_key(std::optional<Key> key) {
  mutex_.lock();
  auto iter = --mapping_.upper_bound(key);
  auto shard = iter->second.get_weak();
  mutex_.unlock();
  return shard;
}

template <class Shard>
void GeneralShardingMapping<Shard>::reserve_new_shard() {
  mutex_.lock();
  auto new_shard = make_proclet_with_capacity<Shard>(proclet_capacity_, self_,
                                                     max_shard_bytes_);
  reserved_shards_.emplace(std::move(new_shard));
  mutex_.unlock();
}

template <class Shard>
WeakProclet<Shard> GeneralShardingMapping<Shard>::create_new_shard(
    std::optional<Key> l_key, std::optional<Key> r_key, bool reserve_space) {
  Proclet<Shard> new_shard;

  if (!reserved_shards_.empty()) {
    mutex_.lock();
    if (likely(!reserved_shards_.empty())) {
      new_shard = std::move(reserved_shards_.top());
      reserved_shards_.pop();
    }
    mutex_.unlock();
  } else {
    new_shard = make_proclet_with_capacity<Shard>(proclet_capacity_, self_,
                                                  max_shard_bytes_, l_key,
                                                  r_key, reserve_space);
  }

  auto new_weak_shard = new_shard.get_weak();

  mutex_.lock();
  mapping_.emplace(l_key, std::move(new_shard));
  mutex_.unlock();

  return new_weak_shard;
}

}  // namespace nu

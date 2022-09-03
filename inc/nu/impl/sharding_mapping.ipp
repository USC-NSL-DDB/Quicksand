namespace nu {

template <class Shard>
GeneralShardingMapping<Shard>::GeneralShardingMapping() : ref_cnt_(1) {}

template <class Shard>
GeneralShardingMapping<Shard>::~GeneralShardingMapping() {
  BUG_ON(ref_cnt_);
}

template <class Shard>
void GeneralShardingMapping<Shard>::seal() {
  ref_cnt_mu_.lock();
  BUG_ON(!ref_cnt_);                        // Cannot be sealed twice.
  while (rt::access_once(ref_cnt_) != 1) {  // Wait until there's only one ref.
    ref_cnt_cv_.wait(&ref_cnt_mu_);
  }
  ref_cnt_ = 0;
  ref_cnt_mu_.unlock();
}

template <class Shard>
void GeneralShardingMapping<Shard>::unseal() {
  ref_cnt_mu_.lock();
  BUG_ON(ref_cnt_);
  ref_cnt_ = 1;
  ref_cnt_mu_.unlock();
  ref_cnt_cv_.signal();  // unblock inc_ref_cnt().
}

template <class Shard>
void GeneralShardingMapping<Shard>::inc_ref_cnt() {
  ref_cnt_mu_.lock();
  while (!rt::access_once(ref_cnt_)) {  // Wait until it is unsealed.
    ref_cnt_cv_.wait(&ref_cnt_mu_);
  }
  ref_cnt_++;
  BUG_ON(!ref_cnt_);
  ref_cnt_mu_.unlock();
}

template <class Shard>
void GeneralShardingMapping<Shard>::dec_ref_cnt() {
  ref_cnt_mu_.lock();
  BUG_ON(!ref_cnt_);
  ref_cnt_--;
  ref_cnt_mu_.unlock();
  ref_cnt_cv_.signal();  // unblock seal().
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

}  // namespace nu

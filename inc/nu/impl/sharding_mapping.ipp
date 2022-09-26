namespace nu {

template <class Shard>
GeneralShardingMapping<Shard>::GeneralShardingMapping(uint64_t proclet_capacity,
                                                      uint32_t max_shard_size)
    : self_(Runtime::get_current_weak_proclet<GeneralShardingMapping>()),
      proclet_capacity_(proclet_capacity),
      max_shard_size_(max_shard_size),
      ref_cnt_(1) {}

template <class Shard>
GeneralShardingMapping<Shard>::~GeneralShardingMapping() {
  BUG_ON(ref_cnt_);
}

template <class Shard>
void GeneralShardingMapping<Shard>::seal() {
  mutex_.lock();
  BUG_ON(!ref_cnt_);                        // Cannot be sealed twice.
  while (rt::access_once(ref_cnt_) != 1) {  // Wait until there's only one ref.
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
  while (!rt::access_once(ref_cnt_)) {  // Wait until it is unsealed.
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

  rw_lock_.reader_lock();
  auto iter = mapping_.upper_bound(l_key);
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
void GeneralShardingMapping<Shard>::reserve_new_shard() {
  auto new_shard = make_proclet_with_capacity<Shard>(proclet_capacity_, self_,
                                                     max_shard_size_);
  reserved_shards_.emplace(std::move(new_shard));
}

template <class Shard>
WeakProclet<Shard> GeneralShardingMapping<Shard>::create_new_shard(
    std::optional<Key> l_key, std::optional<Key> r_key,
    uint64_t container_capacity) {
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
                                                  max_shard_size_, l_key, r_key,
                                                  container_capacity);
  }

  auto new_weak_shard = new_shard.get_weak();

  rw_lock_.writer_lock();
  mapping_.emplace(l_key, std::move(new_shard));
  rw_lock_.writer_unlock();

  return new_weak_shard;
}

}  // namespace nu

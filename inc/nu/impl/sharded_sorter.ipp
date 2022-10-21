namespace nu {

template <typename K, typename V>
inline ShardedSorter<K, V>::ShardedSorter(
    ShardedPairCollection<K, V> &&sharded_pc)
    : sharded_pc_(std::move(sharded_pc)) {}

template <typename K, typename V>
inline ShardedSorter<K, V>::ShardedSorter(ShardedSorter &&o)
    : sharded_pc_(std::move(o.sharded_pc_)) {}

template <typename K, typename V>
inline ShardedSorter<K, V> &ShardedSorter<K, V>::operator=(ShardedSorter &&o) {
  sharded_pc_ = std::move(o.sharded_pc_);
  return *this;
}

template <typename K, typename V>
inline void ShardedSorter<K, V>::emplace(K k, V v) {
  sharded_pc_.emplace(std::move(k), std::move(v));
}

template <typename K, typename V>
inline void ShardedSorter<K, V>::emplace(std::pair<K, V> p) {
  sharded_pc_.emplace(std::move(p));
}

template <typename K, typename V>
ShardedSorted<K, V> ShardedSorter<K, V>::sort() {
  sharded_pc_.for_all_shards(+[](PairCollection<K, V> &pc) { pc.sort(); });
  return nu::to_sealed_ds(std::move(sharded_pc_));
}

template <typename K, typename V>
inline ShardedSorter<K, V> make_sharded_sorter() {
  return ShardedSorter(make_sharded_pair_collection<K, V>());
}

template <typename K, typename V>
inline ShardedSorter<K, V> make_sharded_sorter(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn) {
  return ShardedSorter(
      make_sharded_pair_collection<K, V>(num, estimated_min_key, key_inc_fn));
}
}

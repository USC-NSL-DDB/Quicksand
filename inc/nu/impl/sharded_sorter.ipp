namespace nu {

template <typename K, typename V>
inline ShardedSorter<K, V>::ShardedSorter(ShardedPartitioner<K, V> &&sharded_pn)
    : sharded_pn_(std::move(sharded_pn)) {}

template <typename K, typename V>
inline ShardedSorter<K, V>::ShardedSorter(ShardedSorter &&o)
    : sharded_pn_(std::move(o.sharded_pn_)) {}

template <typename K, typename V>
inline ShardedSorter<K, V> &ShardedSorter<K, V>::operator=(ShardedSorter &&o) {
  sharded_pn_ = std::move(o.sharded_pn_);
  return *this;
}

template <typename K, typename V>
inline void ShardedSorter<K, V>::emplace(K k, V v) {
  sharded_pn_.emplace(std::move(k), std::move(v));
}

template <typename K, typename V>
inline void ShardedSorter<K, V>::emplace(std::pair<K, V> p) {
  sharded_pn_.emplace(std::move(p));
}

template <typename K, typename V>
ShardedSorted<K, V> ShardedSorter<K, V>::sort() {
  sharded_pn_.for_all_shards(+[](Partitioner<K, V> &pn) { pn.sort(); });
  return nu::to_sealed_ds(std::move(sharded_pn_));
}

template <typename K, typename V>
inline ShardedSorter<K, V> make_sharded_sorter() {
  return ShardedSorter(make_sharded_partitioner<K, V>());
}

template <typename K, typename V>
inline ShardedSorter<K, V> make_sharded_sorter(
    uint64_t num, K estimated_min_key,
    std::function<void(K &, uint64_t)> key_inc_fn) {
  return ShardedSorter(
      make_sharded_partitioner<K, V>(num, estimated_min_key, key_inc_fn));
}
}  // namespace nu

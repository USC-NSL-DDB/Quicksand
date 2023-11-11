namespace nu {

template <typename K, typename V>
ShardedSorted<K, V>::ShardedSorted(ShardedPartitioner<K, V> &&sp)
    : ShardedPartitioner<K, V>(std::move(sp)) {}

template <typename K, typename V>
std::vector<std::pair<std::optional<K>, std::size_t>>
ShardedSorted<K, V>::get_all_shards_sizes() {
  auto all_shards_info = ShardedPartitioner<K, V>::get_all_shards_info();
  std::vector<std::pair<std::optional<K>, std::size_t>> ret;
  for (auto &[k, size, _] : all_shards_info) {
    ret.emplace_back(k, size);
  }
  std::ranges::sort(ret);
  return ret;
}

template <typename K, typename V>
inline ShardedSorter<K, V>::ShardedSorter(ShardedPartitioner<K, V> &&sharded_pn)
    : sharded_pn_(std::move(sharded_pn)) {}

template <typename K, typename V>
inline void ShardedSorter<K, V>::insert(K k)
  requires std::is_same_v<V, ErasedType>
{
  sharded_pn_.insert(std::move(k));
}

template <typename K, typename V>
inline void ShardedSorter<K, V>::insert(K k, V v)
  requires(!std::is_same_v<V, ErasedType>)
{
  sharded_pn_.insert(std::move(k), std::move(v));
}

template <typename K, typename V>
inline void ShardedSorter<K, V>::insert(std::pair<K, V> p)
  requires(!std::is_same_v<V, ErasedType>)
{
  sharded_pn_.insert(std::move(p));
}

template <typename K, typename V>
ShardedSorted<K, V> ShardedSorter<K, V>::sort() {
  sharded_pn_.for_all_shards(+[](Partitioner<K, V> &pn) { pn.sort(); });
  return std::move(sharded_pn_);
}

template <typename K, typename V>
template <typename Archive>
void ShardedSorter<K, V>::serialize(Archive &ar) {
  ar(sharded_pn_);
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

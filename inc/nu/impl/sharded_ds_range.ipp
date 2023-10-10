#include "nu/sealed_ds.hpp"

namespace nu {

template <GeneralShardBased Shard>
inline ShardRange<Shard>::ShardRange(WeakProclet<Shard> shard_proclet)
    : shard_proclet_(
          std::make_shared<std::vector<WeakProclet<Shard>>>(1, shard_proclet)),
      begin_(shard_proclet_, true),
      end_(shard_proclet_, false) {}

template <GeneralShardBased Shard>
inline const ShardRange<Shard>::ConstIterator &ShardRange<Shard>::cbegin() const
  requires ConstIterable<Shard>
{
  return begin_;
}

template <GeneralShardBased Shard>
inline const ShardRange<Shard>::ConstIterator &ShardRange<Shard>::cend() const
  requires ConstIterable<Shard>
{
  return end_;
}

template <GeneralShardBased Shard>
inline const ShardRange<Shard>::ConstIterator &ShardRange<Shard>::begin() const
  requires ConstIterable<Shard>
{
  return begin_;
}

template <GeneralShardBased Shard>
inline const ShardRange<Shard>::ConstIterator &ShardRange<Shard>::end() const
  requires ConstIterable<Shard>
{
  return end_;
}

template <GeneralShardBased Shard>
inline ShardRange<Shard> ShardedDSRange<Shard>::pop() {
  return ShardRange(*VectorTaskRange<WeakProclet<Shard>>::pop());
}

template <GeneralShardBased Shard>
inline ShardedDSRange<Shard>::ShardedDSRange(
    VectorTaskRange<WeakProclet<Shard>> &&vector_tr)
    : VectorTaskRange<WeakProclet<Shard>>(std::move(vector_tr)) {}

template <GeneralShardBased Shard>
inline ShardedDSRange<Shard>::ShardedDSRange(
    std::vector<WeakProclet<Shard>> shards)
    : VectorTaskRange<WeakProclet<Shard>>(std::move(shards)) {}

template <ShardedDataStructureBased T>
inline ShardedDSRange<typename T::Shard> make_sharded_ds_range(
    const SealedDS<T> &sealed_ds) {
  return ShardedDSRange<typename T::Shard>(*sealed_ds.shards_);
}

}  // namespace nu

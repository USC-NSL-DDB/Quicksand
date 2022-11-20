#pragma once

#include <memory>
#include <vector>

#include "nu/proclet.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/vector_task_range.hpp"

namespace nu {

template <GeneralShardBased Shard, bool Fwd>
class GeneralSealedDSConstIterator;

template <GeneralShardBased Shard>
class ShardRange {
 public:
  using ConstIterator = GeneralSealedDSConstIterator<Shard, true>;

  ShardRange(WeakProclet<Shard> shard_proclet);
  const ConstIterator &cbegin() const requires ConstIterable<Shard>;
  const ConstIterator &cend() const requires ConstIterable<Shard>;
  const ConstIterator &begin() const requires ConstIterable<Shard>;
  const ConstIterator &end() const requires ConstIterable<Shard>;

 private:
  std::shared_ptr<std::vector<WeakProclet<Shard>>> shard_proclet_;
  ConstIterator begin_;
  ConstIterator end_;
};

template <GeneralShardBased Shard>
class ShardedDSRange : public VectorTaskRange<WeakProclet<Shard>> {
 public:
  using Key = std::size_t;
  using Task = ShardRange<Shard>;

  ShardedDSRange() = default;
  ShardedDSRange(VectorTaskRange<WeakProclet<Shard>> &&vector_tr);
  ShardedDSRange(std::vector<WeakProclet<Shard>> shards);
  ShardedDSRange(const ShardedDSRange &) = default;
  ShardedDSRange &operator=(const ShardedDSRange &) = default;
  ShardedDSRange(ShardedDSRange &&) = default;
  ShardedDSRange &operator=(ShardedDSRange &&) = default;

  Task pop();
};

template <ShardedDataStructureBased T>
class SealedDS;

template <ShardedDataStructureBased T>
ShardedDSRange<typename T::Shard> make_sharded_ds_range(
    const SealedDS<T> &sealed_ds);

}  // namespace nu

#include "nu/impl/sharded_ds_range.ipp"

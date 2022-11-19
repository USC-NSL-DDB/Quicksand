#pragma once

#include <cstddef>
#include <utility>

#include "nu/proclet.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/task_range.hpp"

namespace nu {

template <GeneralShardBased Shard, bool Fwd>
class GeneralSealedDSConstIterator;

template <GeneralShardBased Shard>
class ContiguousDSRangeImpl {
 public:
  using Key = std::size_t;
  using Task = Shard::IterVal;
  using ConstIterator = GeneralSealedDSConstIterator<Shard, true>;
  static_assert(ConstIterator::kContiguous);

  ContiguousDSRangeImpl() = default;
  ContiguousDSRangeImpl(
      std::shared_ptr<std::vector<WeakProclet<Shard>>> &shards,
      std::vector<std::size_t> all_shard_keys, std::size_t size);
  ContiguousDSRangeImpl(const ContiguousDSRangeImpl &) = default;
  ContiguousDSRangeImpl &operator=(const ContiguousDSRangeImpl &) = default;
  ContiguousDSRangeImpl(ContiguousDSRangeImpl &&) = default;
  ContiguousDSRangeImpl &operator=(ContiguousDSRangeImpl &&) = default;
  Task pop();
  std::size_t size() const;
  bool empty() const;
  ContiguousDSRangeImpl split();
  void merge(ContiguousDSRangeImpl r_range);
  std::pair<Key, Key> initial_key_range() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  ConstIterator cur_;
  ConstIterator end_;
  std::vector<std::size_t> all_shard_keys_;
  std::pair<std::size_t, std::size_t> initial_key_range_;

  std::size_t get_cur_key();
};

template <GeneralShardBased Shard>
using ContiguousDSRange = TaskRange<ContiguousDSRangeImpl<Shard>>;

template <ShardedDataStructureBased T>
class SealedDS;

template <ShardedDataStructureBased T>
ContiguousDSRange<typename T::Shard>
make_contiguous_ds_range(const SealedDS<T> &sealed_ds) requires(
    SealedDS<T>::ConstIterator::kContiguous);

}  // namespace nu

#include "nu/impl/cont_ds_range.ipp"

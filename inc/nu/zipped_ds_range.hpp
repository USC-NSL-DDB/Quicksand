#pragma once

#include <tuple>

#include "nu/cont_ds_range.hpp"

namespace nu {

template <GeneralShardBased... Shards>
class ZippedDSRangeImpl {
 public:
  using Key = std::size_t;
  using Task = std::tuple<typename Shards::IterVal...>;

  ZippedDSRangeImpl() = default;
  ZippedDSRangeImpl(std::tuple<ContiguousDSRange<Shards>...> cont_ds_ranges);
  ZippedDSRangeImpl(const ZippedDSRangeImpl &) = default;
  ZippedDSRangeImpl &operator=(const ZippedDSRangeImpl &) = default;
  ZippedDSRangeImpl(ZippedDSRangeImpl &&) = default;
  ZippedDSRangeImpl &operator=(ZippedDSRangeImpl &&) = default;
  Task pop();
  ZippedDSRangeImpl split(uint64_t last_n_elems);
  Key l_key() const;
  std::size_t initial_size() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::tuple<ContiguousDSRange<Shards>...> cont_ds_ranges_;
};

template <GeneralShardBased... Shards>
using ZippedDSRange = TaskRange<ZippedDSRangeImpl<Shards...>>;

template <ShardedDataStructureBased T>
class SealedDS;

template <ShardedDataStructureBased... Ts>
ZippedDSRange<typename Ts::Shard...>
make_zipped_ds_range(const SealedDS<Ts> &... sealed_dses) requires(
    ((SealedDS<Ts>::ConstIterator::kContiguous) && ...));

}  // namespace nu

#include "nu/impl/zipped_ds_range.ipp"

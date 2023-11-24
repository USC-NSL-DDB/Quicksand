#include <algorithm>
#include <utility>

namespace nu {

template <GeneralShardBased... Shards>
inline ZippedDSRangeImpl<Shards...>::ZippedDSRangeImpl(
    std::tuple<ContiguousDSRange<Shards>...> cont_ds_ranges)
    : cont_ds_ranges_(std::move(cont_ds_ranges)) {
  auto l_key = std::get<0>(cont_ds_ranges_).l_key();
  auto size = std::get<0>(cont_ds_ranges_).size();
  std::apply(
      [&](auto &...cont_ds_ranges) {
        BUG_ON(((l_key != cont_ds_ranges.l_key()) | ...));
        BUG_ON(((size != cont_ds_ranges.size()) | ...));
      },
      cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
inline ZippedDSRangeImpl<Shards...> ZippedDSRangeImpl<Shards...>::deep_copy() {
  return ZippedDSRangeImpl(std::apply(
      [](auto &...cont_ds_ranges) {
        return std::make_tuple(cont_ds_ranges.deep_copy()...);
      },
      cont_ds_ranges_));
}

template <GeneralShardBased... Shards>
inline std::tuple<typename Shards::IterVal...>
ZippedDSRangeImpl<Shards...>::pop() {
  return std::apply(
      [](auto &...cont_ds_ranges) {
        return std::make_tuple(*cont_ds_ranges.pop()...);
      },
      cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
inline Lazy<ZippedDSRangeImpl<Shards...>> ZippedDSRangeImpl<Shards...>::split(
    uint64_t last_n_elems) {
  auto tuple_of_lazys = std::apply(
      [&](auto &...cols) {
        return std::make_tuple(cols.split(last_n_elems)...);
      },
      cont_ds_ranges_);
  return make_lazy([tuple_of_lazys = std::move(tuple_of_lazys)]() mutable {
    return std::apply(
        [&](auto &...lazy) {
          return ZippedDSRangeImpl(std::make_tuple(std::move(lazy.get())...));
        },
        tuple_of_lazys);
  });
}

template <GeneralShardBased... Shards>
inline std::size_t ZippedDSRangeImpl<Shards...>::l_key() const {
  return std::get<0>(cont_ds_ranges_).l_key();
}

template <GeneralShardBased... Shards>
inline std::size_t ZippedDSRangeImpl<Shards...>::initial_size() const {
  return std::get<0>(cont_ds_ranges_).impl().initial_size();
}

template <GeneralShardBased... Shards>
template <class Archive>
inline void ZippedDSRangeImpl<Shards...>::save(Archive &ar) const {
  std::apply([&](auto &...cont_ds_ranges) { ar(cont_ds_ranges...); },
             cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
template <class Archive>
inline void ZippedDSRangeImpl<Shards...>::load(Archive &ar) {
  std::apply([&](auto &...cont_ds_ranges) { ar(cont_ds_ranges...); },
             cont_ds_ranges_);
}

template <ShardedDataStructureBased... Ts>
inline ZippedDSRange<typename Ts::Shard...> make_zipped_ds_range(
    const SealedDS<Ts> &...sealed_dses)
  requires(((SealedDS<Ts>::ConstIterator::kContiguous) && ...))
{
  return ZippedDSRange<typename Ts::Shard...>(
      ZippedDSRangeImpl<typename Ts::Shard...>(
          std::make_tuple(make_contiguous_ds_range(sealed_dses)...)));
}

}  // namespace nu

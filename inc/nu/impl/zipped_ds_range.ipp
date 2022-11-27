#include <algorithm>
#include <utility>

namespace nu {

template <GeneralShardBased... Shards>
inline ZippedDSRangeImpl<Shards...>::ZippedDSRangeImpl(
    std::tuple<ContiguousDSRange<Shards>...> cont_ds_ranges)
    : cont_ds_ranges_(std::move(cont_ds_ranges)) {
  auto initial_key_range = std::get<0>(cont_ds_ranges_).initial_key_range();
  std::apply(
      [&](auto &... cont_ds_ranges) {
        BUG_ON(((initial_key_range != cont_ds_ranges.initial_key_range()) |
                ...));
      },
      cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
inline std::tuple<typename Shards::IterVal...>
ZippedDSRangeImpl<Shards...>::pop() {
  return std::apply(
      [](auto &... cont_ds_ranges) {
        return std::make_tuple(cont_ds_ranges.pop()...);
      },
      cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
inline std::size_t ZippedDSRangeImpl<Shards...>::size() const {
  return std::get<0>(cont_ds_ranges_).size();
}

template <GeneralShardBased... Shards>
inline bool ZippedDSRangeImpl<Shards...>::empty() const {
  return std::get<0>(cont_ds_ranges_).empty();
}

template <GeneralShardBased... Shards>
inline ZippedDSRangeImpl<Shards...> ZippedDSRangeImpl<Shards...>::split() {
  auto first_split = std::get<0>(cont_ds_ranges_).split();
  if (unlikely(first_split.empty())) {
    return ZippedDSRangeImpl();
  }

  auto mid_key = first_split.initial_key_range().first;
  return ZippedDSRangeImpl(std::apply(
      [&](auto &first, auto &... others) {
        return std::make_tuple(std::move(first_split),
                               others.impl().__split(mid_key).value()...);
      },
      cont_ds_ranges_));
}

template <GeneralShardBased... Shards>
inline std::pair<std::size_t, std::size_t>
ZippedDSRangeImpl<Shards...>::initial_key_range() const {
  return std::get<0>(cont_ds_ranges_).initial_key_range();
}

template <GeneralShardBased... Shards>
template <class Archive>
inline void ZippedDSRangeImpl<Shards...>::save(Archive &ar) const {
  std::apply([&](auto &... cont_ds_ranges) { ar(cont_ds_ranges...); },
             cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
template <class Archive>
inline void ZippedDSRangeImpl<Shards...>::load(Archive &ar) {
  std::apply([&](auto &... cont_ds_ranges) { ar(cont_ds_ranges...); },
             cont_ds_ranges_);
}

template <ShardedDataStructureBased... Ts>
inline ZippedDSRange<typename Ts::Shard...>
make_zipped_ds_range(const SealedDS<Ts> &... sealed_dses) requires(
    ((SealedDS<Ts>::ConstIterator::kContiguous) && ...)) {
  return ZippedDSRange<typename Ts::Shard...>(
      ZippedDSRangeImpl<typename Ts::Shard...>(
          std::make_tuple(make_contiguous_ds_range(sealed_dses)...)));
}

}  // namespace nu

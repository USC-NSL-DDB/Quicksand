#include <algorithm>
#include <utility>

namespace nu {

template <GeneralShardBased... Shards>
ZippedDSRangeImpl<Shards...>::ZippedDSRangeImpl(
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
std::tuple<typename Shards::IterVal...> ZippedDSRangeImpl<Shards...>::pop() {
  return std::apply(
      [](auto &... cont_ds_ranges) {
        return std::make_tuple(cont_ds_ranges.pop()...);
      },
      cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
std::size_t ZippedDSRangeImpl<Shards...>::size() const {
  return std::get<0>(cont_ds_ranges_).size();
}

template <GeneralShardBased... Shards>
bool ZippedDSRangeImpl<Shards...>::empty() const {
  return std::get<0>(cont_ds_ranges_).empty();
}

template <GeneralShardBased... Shards>
ZippedDSRangeImpl<Shards...> ZippedDSRangeImpl<Shards...>::split() {
  return ZippedDSRangeImpl(std::apply(
      [](auto &... cont_ds_ranges) {
        return std::make_tuple(cont_ds_ranges.split()...);
      },
      cont_ds_ranges_));
}

template <GeneralShardBased... Shards>
void ZippedDSRangeImpl<Shards...>::merge(ZippedDSRangeImpl r_range) {
  [&]<typename T, T... Ints>(std::integer_sequence<T, Ints...> seqs) {
    ((std::get<Ints>(cont_ds_ranges_)
          .merge(std::move(std::get<Ints>(r_range.cont_ds_ranges_)))),
     ...);
  }
  (std::make_index_sequence<sizeof...(Shards)>{});
}

template <GeneralShardBased... Shards>
std::pair<std::size_t, std::size_t>
ZippedDSRangeImpl<Shards...>::initial_key_range() const {
  return std::get<0>(cont_ds_ranges_).initial_key_range();
}

template <GeneralShardBased... Shards>
template <class Archive>
void ZippedDSRangeImpl<Shards...>::save(Archive &ar) const {
  std::apply([&](auto &... cont_ds_ranges) { ar(cont_ds_ranges...); },
             cont_ds_ranges_);
}

template <GeneralShardBased... Shards>
template <class Archive>
void ZippedDSRangeImpl<Shards...>::load(Archive &ar) {
  std::apply([&](auto &... cont_ds_ranges) { ar(cont_ds_ranges...); },
             cont_ds_ranges_);
}

template <ShardedDataStructureBased... Ts>
ZippedDSRange<typename Ts::Shard...>
make_zipped_ds_range(const SealedDS<Ts> &... sealed_dses) requires(
    ((SealedDS<Ts>::ConstIterator::kContiguous) && ...)) {
  return ZippedDSRange<typename Ts::Shard...>(
      ZippedDSRangeImpl<typename Ts::Shard...>(
          std::make_tuple(make_contiguous_ds_range(sealed_dses)...)));
}

}  // namespace nu

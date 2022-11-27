#include <algorithm>
#include <ranges>

namespace nu {

template <class Shard>
inline ContiguousDSRangeImpl<Shard>::ContiguousDSRangeImpl()
    : initial_key_range_(0, 0), cur_key_(0) {}

template <class Shard>
inline ContiguousDSRangeImpl<Shard>::ContiguousDSRangeImpl(
    std::shared_ptr<std::vector<WeakProclet<Shard>>> &shards,
    std::vector<std::size_t> all_shard_keys, std::size_t size)
    : cur_(shards, true),
      all_shard_keys_(std::move(all_shard_keys)),
      initial_key_range_(0, size),
      cur_key_(0) {}

template <class Shard>
inline Shard::IterVal ContiguousDSRangeImpl<Shard>::pop() {
  auto ret = std::move(*cur_);
  ++cur_;
  ++cur_key_;
  return ret;
}

template <class Shard>
inline std::size_t ContiguousDSRangeImpl<Shard>::size() const {
  auto end_key = rt::access_once(initial_key_range_.second);
  auto cur_key = cur_key_;
  if (end_key >= cur_key) {
    return end_key - cur_key;
  } else {
    return 0;
  }
}

template <class Shard>
inline bool ContiguousDSRangeImpl<Shard>::empty() const {
  return !size();
}

template <class Shard>
inline ContiguousDSRangeImpl<Shard> ContiguousDSRangeImpl<Shard>::split() {
retry:
  auto mid_key = (cur_key_ + initial_key_range_.second) / 2;
  auto optional = __split(mid_key);
  if (unlikely(!optional)) {
    goto retry;
  }
  return std::move(*optional);
}

template <class Shard>
std::optional<ContiguousDSRangeImpl<Shard>>
ContiguousDSRangeImpl<Shard>::__split(std::size_t mid_key) {
  auto end_key = initial_key_range_.second;
  initial_key_range_.second = mid_key;
  mb();
  if (unlikely(cur_key_ >= mid_key)) {
    initial_key_range_.second = end_key;
    if (unlikely(cur_key_ + 1 >= end_key)) {
      return ContiguousDSRangeImpl();
    }
    return std::nullopt;
  }

  ContiguousDSRangeImpl r_range;
  auto mid_shard_idx = std::upper_bound(all_shard_keys_.begin(),
                                        all_shard_keys_.end(), mid_key) -
                       all_shard_keys_.begin() - 1;
  auto mid_shard_iter = cur_.shards_->begin() + mid_shard_idx;
  auto find_tuple = mid_shard_iter->run(&Shard::find, mid_key);
  BUG_ON(!std::get<0>(find_tuple));
  r_range.cur_ =
      ConstIterator(cur_.shards_, mid_shard_iter, std::get<1>(find_tuple),
                    std::get<2>(find_tuple));
  r_range.all_shard_keys_ = all_shard_keys_;
  r_range.initial_key_range_ = std::make_pair(mid_key, end_key);
  r_range.cur_key_ = mid_key;

  return r_range;
}

template <class Shard>
template <class Archive>
inline void ContiguousDSRangeImpl<Shard>::save(Archive &ar) const {
  ar(cur_, all_shard_keys_, initial_key_range_, cur_key_);
}

template <class Shard>
template <class Archive>
inline void ContiguousDSRangeImpl<Shard>::load(Archive &ar) {
  ar(cur_, all_shard_keys_, initial_key_range_, cur_key_);
}

template <class Shard>
inline std::pair<std::size_t, std::size_t>
ContiguousDSRangeImpl<Shard>::initial_key_range() const {
  return initial_key_range_;
}

template <ShardedDataStructureBased T>
inline ContiguousDSRange<typename T::Shard>
make_contiguous_ds_range(const SealedDS<T> &sealed_ds) requires(
    SealedDS<T>::ConstIterator::kContiguous) {
  std::vector<std::size_t> all_shard_keys;
  std::ranges::transform(
      sealed_ds.keys_, std::back_inserter(all_shard_keys),
      [](const auto &optional_key) { return optional_key.value_or(0); });
  return ContiguousDSRange(ContiguousDSRangeImpl<typename T::Shard>(
      const_cast<SealedDS<T> &>(sealed_ds).shards_, std::move(all_shard_keys),
      sealed_ds.prefix_sum_sizes_.back()));
}

}  // namespace nu

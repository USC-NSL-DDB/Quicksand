#include <algorithm>
#include <ranges>

namespace nu {

template <class Shard>
ContiguousDSRangeImpl<Shard>::ContiguousDSRangeImpl(
    std::shared_ptr<std::vector<WeakProclet<Shard>>> &shards,
    std::vector<std::size_t> all_shard_keys, std::size_t size)
    : cur_(shards, true),
      end_(shards, false),
      all_shard_keys_(std::move(all_shard_keys)),
      initial_key_range_(0, size) {}

template <class Shard>
Shard::IterVal ContiguousDSRangeImpl<Shard>::pop() {
  auto ret = std::move(*cur_);
  ++cur_;
  return ret;
}

template <class Shard>
std::size_t ContiguousDSRangeImpl<Shard>::get_cur_key() {
  return cur_.shards_iter_->run(
      +[](Shard &shard, ConstIterator::ContainerIter container_iter) {
        auto l_key = *shard.l_key_;
        return l_key + (container_iter - shard.container_.cbegin());
      },
      cur_.to_gid());
}

template <class Shard>
std::size_t ContiguousDSRangeImpl<Shard>::size() const {
  if (unlikely(empty())) {
    return 0;
  }
  return initial_key_range_.second -
         const_cast<ContiguousDSRangeImpl *>(this)->get_cur_key();
}

template <class Shard>
bool ContiguousDSRangeImpl<Shard>::empty() const {
  return cur_ == end_;
}

template <class Shard>
ContiguousDSRangeImpl<Shard> ContiguousDSRangeImpl<Shard>::split() {
  ContiguousDSRangeImpl r_range;

  auto cur_key = get_cur_key();
  auto end_key = initial_key_range_.second;
  auto mid_key = (cur_key + end_key) / 2;
  auto mid_shard_idx = std::upper_bound(all_shard_keys_.begin(),
                                        all_shard_keys_.end(), mid_key) -
                       all_shard_keys_.begin() - 1;
  auto mid_shard_iter = cur_.shards_->begin() + mid_shard_idx;
  auto find_tuple = mid_shard_iter->run(+Shard::find, mid_key);
  BUG_ON(!std::get<0>(find_tuple));
  r_range.end_ = std::move(end_);
  end_ = ConstIterator(cur_.shards_, mid_shard_iter, std::get<1>(find_tuple),
                       std::get<2>(find_tuple));
  r_range.cur_ = end_;
  r_range.all_shard_keys_ = all_shard_keys_;
  r_range.initial_key_ranges_ = std::make_pair(mid_key, end_key);
  initial_key_range_.second = mid_key;

  return r_range;
}

template <class Shard>
void ContiguousDSRangeImpl<Shard>::merge(ContiguousDSRangeImpl<Shard> r_range) {
  BUG_ON(initial_key_range_.second != r_range.initial_key_range_.first);
  end_ = std::move(r_range.end_);
  initial_key_range_.second = r_range.initial_key_range_.second;
}

template <class Shard>
template <class Archive>
void ContiguousDSRangeImpl<Shard>::save(Archive &ar) const {
  ar(cur_, end_, all_shard_keys_, initial_key_range_);
}

template <class Shard>
template <class Archive>
void ContiguousDSRangeImpl<Shard>::load(Archive &ar) {
  ar(cur_, end_, all_shard_keys_, initial_key_range_);
}

template <class Shard>
std::pair<std::size_t, std::size_t>
ContiguousDSRangeImpl<Shard>::initial_key_range() const {
  return initial_key_range_;
}

template <ShardedDataStructureBased T>
ContiguousDSRange<typename T::Shard>
make_contiguous_ds_range(const SealedDS<T> &sealed_ds) requires(
    SealedDS<T>::ConstIterator::kContiguous) {
  std::vector<std::size_t> all_shard_keys;
  std::ranges::transform(sealed_ds.keys_, std::back_inserter(all_shard_keys),
                         [](auto optional_key) { return *optional_key; });
  return ContiguousDSRange(ContiguousDSRangeImpl<typename T::Shard>(
      const_cast<SealedDS<T> &>(sealed_ds).shards_, std::move(all_shard_keys),
      sealed_ds.prefix_sum_sizes_.back()));
}

}  // namespace nu

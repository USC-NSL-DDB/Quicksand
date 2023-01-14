#include <algorithm>
#include <ranges>

namespace nu {

template <class Shard>
inline ContiguousDSRangeImpl<Shard> ContiguousDSRangeImpl<Shard>::deep_copy() {
  ContiguousDSRangeImpl impl;

  impl.cur_ = cur_.deep_copy();
  impl.all_shard_keys_ = all_shard_keys_;
  impl.l_key_ = l_key_;
  impl.r_key_ = r_key_;

  return impl;
}

template <class Shard>
inline ContiguousDSRangeImpl<Shard>::ContiguousDSRangeImpl()
    : l_key_(0), r_key_(0) {}

template <class Shard>
inline ContiguousDSRangeImpl<Shard>::ContiguousDSRangeImpl(
    std::shared_ptr<std::vector<WeakProclet<Shard>>> &shards,
    std::vector<std::size_t> all_shard_keys, std::size_t size)
    : cur_(shards, true),
      all_shard_keys_(std::move(all_shard_keys)),
      l_key_(0),
      r_key_(size) {}

template <class Shard>
inline Shard::IterVal ContiguousDSRangeImpl<Shard>::pop() {
  auto ret = cur_.move_deref();
  ++cur_;
  return ret;
}

template <class Shard>
inline ContiguousDSRangeImpl<Shard> ContiguousDSRangeImpl<Shard>::split(
    uint64_t last_n_elems) {
  ContiguousDSRangeImpl r_range;
  auto split_key = r_key_ - last_n_elems;
  auto split_shard_idx = std::upper_bound(all_shard_keys_.begin(),
                                          all_shard_keys_.end(), split_key) -
                         all_shard_keys_.begin() - 1;
  auto split_shard_iter = cur_.shards_->begin() + split_shard_idx;
  auto find = split_shard_iter->run(&Shard::find, split_key);
  r_range.cur_ =
      ConstIterator(cur_.shards_, split_shard_iter, find.first, find.second);
  r_range.all_shard_keys_ = all_shard_keys_;
  r_range.l_key_ = split_key;
  r_range.r_key_ = r_key_;
  r_key_ = split_key;

  return r_range;
}

template <class Shard>
template <class Archive>
inline void ContiguousDSRangeImpl<Shard>::save(Archive &ar) const {
  ar(cur_, all_shard_keys_, l_key_, r_key_);
}

template <class Shard>
template <class Archive>
inline void ContiguousDSRangeImpl<Shard>::load(Archive &ar) {
  ar(cur_, all_shard_keys_, l_key_, r_key_);
}

template <class Shard>
inline std::size_t ContiguousDSRangeImpl<Shard>::l_key() const {
  return l_key_;
}

template <class Shard>
inline std::size_t ContiguousDSRangeImpl<Shard>::initial_size() const {
  return r_key_ - l_key_;
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

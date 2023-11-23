#include <algorithm>
#include <ranges>

#include "nu/cereal.hpp"

namespace nu {

template <typename T>
inline VectorTaskRangeImpl<T>::VectorTaskRangeImpl() : cur_key_(0), l_key_(0) {}

template <typename T>
inline VectorTaskRangeImpl<T>::VectorTaskRangeImpl(std::vector<T> tasks)
    : tasks_(std::move(tasks)), cur_key_(0), l_key_(0) {}

template <typename T>
inline T VectorTaskRangeImpl<T>::pop() {
  return std::move(tasks_[cur_key_++]);
}

template <typename T>
inline Lazy<VectorTaskRangeImpl<T>> VectorTaskRangeImpl<T>::split(
    uint64_t last_n_elems) {
  VectorTaskRangeImpl r_range;
  auto split_idx = tasks_.size() - last_n_elems;
  std::ranges::move(tasks_.begin() + split_idx, tasks_.end(),
                    std::back_inserter(r_range.tasks_));
  tasks_.resize(split_idx);
  r_range.cur_key_ = 0;
  r_range.l_key_ = l_key_ + split_idx;
  return make_lazy(std::move(r_range));
}

template <typename T>
inline std::size_t VectorTaskRangeImpl<T>::l_key() const {
  return l_key_;
}

template <typename T>
inline std::size_t VectorTaskRangeImpl<T>::initial_size() const {
  return tasks_.size();
}

template <typename T>
template <class Archive>
inline void VectorTaskRangeImpl<T>::save(Archive &ar) const {
  ar(tasks_, cur_key_, l_key_);
}

template <typename T>
template <class Archive>
inline void VectorTaskRangeImpl<T>::load(Archive &ar) {
  ar(tasks_, cur_key_, l_key_);
}

}  // namespace nu

#include <algorithm>
#include <ranges>

#include "nu/cereal.hpp"

namespace nu {

template <typename T>
inline VectorTaskRangeImpl<T>::VectorTaskRangeImpl()
    : cur_idx_(0), end_idx_(0), key_offset_(0) {}

template <typename T>
inline VectorTaskRangeImpl<T>::VectorTaskRangeImpl(std::vector<T> tasks)
    : tasks_(std::move(tasks)),
      cur_idx_(0),
      end_idx_(tasks_.size()),
      key_offset_(0) {}

template <typename T>
inline T VectorTaskRangeImpl<T>::pop() {
  return std::move(tasks_[cur_idx_++]);
}

template <typename T>
inline std::size_t VectorTaskRangeImpl<T>::size() const {
  auto end_idx = rt::access_once(end_idx_);
  auto cur_idx = cur_idx_;
  if (likely(end_idx >= cur_idx)) {
    return end_idx - cur_idx;
  } else {
    return 0;
  }
}

template <typename T>
inline bool VectorTaskRangeImpl<T>::empty() const {
  return !size();
}

template <typename T>
inline VectorTaskRangeImpl<T> VectorTaskRangeImpl<T>::split() {
retry:
  auto orig_end_idx = end_idx_;
  auto mid_idx = (cur_idx_ + end_idx_) / 2;
  end_idx_ = mid_idx;
  mb();
  if (unlikely(cur_idx_ >= mid_idx)) {
    end_idx_ = orig_end_idx;
    if (unlikely(cur_idx_ + 1 >= end_idx_)) {
      return VectorTaskRangeImpl();
    }
    goto retry;
  }

  VectorTaskRangeImpl r_range;
  std::ranges::move(tasks_.begin() + mid_idx, tasks_.end(),
                    std::back_inserter(r_range.tasks_));
  tasks_.resize(mid_idx);
  r_range.cur_idx_ = 0;
  r_range.end_idx_ = r_range.tasks_.size();
  r_range.key_offset_ = key_offset_ + mid_idx;
  return r_range;
}

template <typename T>
inline std::pair<std::size_t, std::size_t>
VectorTaskRangeImpl<T>::initial_key_range() const {
  return std::make_pair(key_offset_, end_idx_ + key_offset_);
}

template <typename T>
template <class Archive>
inline void VectorTaskRangeImpl<T>::save(Archive &ar) const {
  ar(tasks_, cur_idx_, end_idx_, key_offset_);
}

template <typename T>
template <class Archive>
inline void VectorTaskRangeImpl<T>::load(Archive &ar) {
  ar(tasks_, cur_idx_, end_idx_, key_offset_);
}

}  // namespace nu

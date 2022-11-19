#include <algorithm>
#include <ranges>

namespace nu {

template <typename T>
inline VectorTaskRangeImpl<T>::VectorTaskRangeImpl()
    : idx_(0), key_offset_(0) {}

template <typename T>
inline VectorTaskRangeImpl<T>::VectorTaskRangeImpl(std::vector<T> tasks)
    : tasks_(std::move(tasks)), idx_(0), key_offset_(0) {}

template <typename T>
inline T VectorTaskRangeImpl<T>::pop() {
  return std::move(tasks_[idx_++]);
}

template <typename T>
inline std::size_t VectorTaskRangeImpl<T>::size() const {
  return tasks_.size() - idx_;
}

template <typename T>
inline bool VectorTaskRangeImpl<T>::empty() const {
  return !size();
}

template <typename T>
inline VectorTaskRangeImpl<T> VectorTaskRangeImpl<T>::split() {
  VectorTaskRangeImpl new_range;
  auto mid = idx_ + size() / 2;
  std::ranges::move(tasks_.begin() + mid, tasks_.end(),
                    std::back_inserter(new_range.tasks_));
  tasks_.resize(mid - idx_);
  new_range.idx_ = 0;
  new_range.offset_ = key_offset_ + tasks_.size();
  return new_range;
}

template <typename T>
inline void VectorTaskRangeImpl<T>::merge(VectorTaskRangeImpl task_range) {
  BUG_ON(key_offset_ + tasks_.size() != task_range.key_offset_);
  std::ranges::move(task_range.tasks_, std::back_inserter(tasks_));
}

template <typename T>
inline std::pair<std::size_t, std::size_t>
VectorTaskRangeImpl<T>::initial_key_range() const {
  return std::make_pair(key_offset_, tasks_.size() + key_offset_);
}

template <typename T>
template <class Archive>
inline void VectorTaskRangeImpl<T>::save(Archive &ar) const {
  ar(tasks_, idx_, key_offset_);
}

template <typename T>
template <class Archive>
inline void VectorTaskRangeImpl<T>::load(Archive &ar) {
  ar(tasks_, idx_, key_offset_);
}

}  // namespace nu

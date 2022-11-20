#pragma once

#include <vector>

#include "nu/task_range.hpp"

namespace nu {

template <typename T>
class VectorTaskRangeImpl {
 public:
  using Key = std::size_t;
  using Task = T;

  VectorTaskRangeImpl();
  VectorTaskRangeImpl(std::vector<T> tasks);
  VectorTaskRangeImpl(const VectorTaskRangeImpl &) = default;
  VectorTaskRangeImpl &operator=(const VectorTaskRangeImpl &) = default;
  VectorTaskRangeImpl(VectorTaskRangeImpl &&) = default;
  VectorTaskRangeImpl &operator=(VectorTaskRangeImpl &&) = default;
  T pop();
  ssize_t size() const;
  bool empty() const;
  VectorTaskRangeImpl split();
  std::pair<Key, Key> initial_key_range() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::vector<T> tasks_;
  std::size_t cur_idx_;
  std::size_t end_idx_;
  std::size_t key_offset_;
};

template <typename T>
using VectorTaskRange = TaskRange<VectorTaskRangeImpl<T>>;

}  // namespace nu

#include "nu/impl/vector_task_range.ipp"

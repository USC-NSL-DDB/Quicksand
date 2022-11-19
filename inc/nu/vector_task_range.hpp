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
  std::size_t size() const;
  bool empty() const;
  VectorTaskRangeImpl split();
  void merge(VectorTaskRangeImpl r_range);
  std::pair<Key, Key> initial_key_range() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::vector<T> tasks_;
  size_t idx_;
  std::size_t key_offset_;
};

template <typename T>
using VectorTaskRange = TaskRange<VectorTaskRangeImpl<T>>;

}  // namespace nu

#include "nu/impl/vector_task_range.ipp"

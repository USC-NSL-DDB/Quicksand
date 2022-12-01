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
  VectorTaskRangeImpl split(uint64_t last_n_elems);
  Key l_key() const;
  std::size_t initial_size() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::vector<T> tasks_;
  std::size_t cur_key_;
  std::size_t l_key_;
};

template <typename T>
using VectorTaskRange = TaskRange<VectorTaskRangeImpl<T>>;

}  // namespace nu

#include "nu/impl/vector_task_range.ipp"

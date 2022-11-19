#pragma once

#include <cstddef>
#include <utility>

#include "nu/type_traits.hpp"

namespace nu {

template <class Impl>
class TaskRange {
 public:
  using Key = Impl::Key;
  using Task = Impl::Task;

  TaskRange() = default;
  TaskRange(Impl impl) : impl_(std::move(impl)) {}
  TaskRange(const TaskRange &) = default;
  TaskRange &operator=(const TaskRange &) = default;
  TaskRange(TaskRange &&) = default;
  TaskRange &operator=(TaskRange &&) = default;
  Task pop() { return impl_.pop(); }
  std::size_t size() const { return impl_.size(); }
  bool empty() const { return impl_.empty(); }
  TaskRange split() { return impl_.split(); }
  void merge(TaskRange r_range) { impl_.merge(r_range); }
  std::pair<Key, Key> initial_key_range() const {
    return impl_.initial_key_range();
  }
  template <class Archive>
  void save(Archive &ar) const {
    ar(impl_);
  }
  template <class Archive>
  void load(Archive &ar) {
    ar(impl_);
  }

 private:
  Impl impl_;
};

template <class T>
concept TaskRangeBased = requires {
  requires is_base_of_template_v<T, TaskRange>;
};

}  // namespace nu

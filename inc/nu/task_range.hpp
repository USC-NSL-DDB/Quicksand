#pragma once

#include <atomic>
#include <cstddef>
#include <utility>

#include <sync.h>

#include "nu/type_traits.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <class Impl>
class TaskRange;

template <class T>
concept TaskRangeBased = requires {
  requires is_base_of_template_v<T, TaskRange>;
};;

template <class Impl>
class TaskRange {
 public:
  using Key = Impl::Key;
  using Task = Impl::Task;

  TaskRange() : impl_() {}
  TaskRange(Impl impl) : impl_(std::move(impl)) {}
  TaskRange(const TaskRange &o) = default;
  TaskRange &operator=(const TaskRange &o) = default;
  TaskRange(TaskRange &&o) = default;
  TaskRange &operator=(TaskRange &&o) = default;
  ~TaskRange() {}
  Task pop() { return impl_.pop(); }
  std::size_t size() const { return impl_.size(); }
  bool empty() const { return impl_.empty(); }
  TaskRange split() { return impl_.split(); }
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
  Impl &impl() { return impl_; }

 private:
  Impl impl_;
  template <TaskRangeBased TR, typename... States>
  friend class ComputeProcletWorker;
};

}  // namespace nu


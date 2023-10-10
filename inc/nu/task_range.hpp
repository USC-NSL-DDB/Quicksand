#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>

#include "nu/type_traits.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <class Impl>
class TaskRange;

template <class T>
concept TaskRangeBased =
    requires { requires is_base_of_template_v<T, TaskRange>; };

template <class Impl>
class TaskRangeIterator;

template <class Impl>
class TaskRange {
 public:
  using Key = Impl::Key;
  using Task = Impl::Task;
  using Implementation = Impl;

  TaskRange() : impl_() {}
  TaskRange(Impl impl) : impl_(std::move(impl)), size_(impl_.initial_size()) {}
  TaskRange(const TaskRange &o);
  TaskRange &operator=(const TaskRange &o);
  TaskRange(TaskRange &&o) noexcept;
  TaskRange &operator=(TaskRange &&o) noexcept;
  ~TaskRange();
  TaskRange deep_copy();
  template <typename F>
  void run(F &&f);
  std::optional<Task> pop();
  void clear();
  void suspend();
  void resume();
  std::size_t size() const { return size_; }
  std::size_t processed_size() const { return processed_size_; }
  bool empty() const { return Caladan::access_once(cleared_) || !size(); }
  TaskRange split(uint64_t last_n_elems);
  TaskRange steal();
  Key l_key() const { return impl_.l_key(); }
  template <class Archive>
  void save(Archive &ar) const {
    ar(impl_, size_);
  }
  template <class Archive>
  void load(Archive &ar) {
    ar(impl_, size_);
  }
  Impl &impl() { return impl_; }
  const Impl &impl() const { return impl_; }
  TaskRangeIterator<Impl> begin() const;
  TaskRangeIterator<Impl> end() const;

 private:
  Impl impl_;
  std::size_t size_ = 0;
  std::size_t processed_size_ = 0;
  bool pending_steal_ = false;
  bool cleared_ = false;
  bool suspended_ = false;
  uint64_t steal_size_;
  CondVar cv_;
  CondVar suspend_cv_;
  Mutex mutex_;
  template <TaskRangeBased TR, typename... States>
  friend class ComputeProclet;

  void cleanup_steal();
  void __resume();
};

template <class Impl>
class TaskRangeIterator {
 public:
  using Task = TaskRange<Impl>::Task;

  TaskRangeIterator(TaskRange<Impl> &range, bool end = false);
  bool operator==(const TaskRangeIterator<Impl> &o);
  Task operator*() const;
  TaskRangeIterator<Impl> &operator++();

 private:
  TaskRange<Impl> &range_;
  std::optional<Task> curr_;
};

}  // namespace nu

#include "nu/impl/task_range.ipp"

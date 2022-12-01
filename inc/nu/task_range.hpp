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
};

template <class Impl>
class TaskRange {
 public:
  using Key = Impl::Key;
  using Task = Impl::Task;

  TaskRange() : impl_() {}
  TaskRange(Impl impl) : impl_(std::move(impl)), size_(impl_.initial_size()) {}
  TaskRange(const TaskRange &o);
  TaskRange &operator=(const TaskRange &o);
  TaskRange(TaskRange &&o) noexcept;
  TaskRange &operator=(TaskRange &&o) noexcept;
  ~TaskRange();
  template <typename F>
  void run(F &&f);
  Task pop();
  std::size_t size() const { return size_; }
  bool empty() const { return !size(); }
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

 private:
  Impl impl_;
  std::size_t size_ = 0;
  bool pending_steal_ = false;
  uint64_t steal_size_;
  CondVar cv_;
  Mutex mutex_;
  template <TaskRangeBased TR, typename... States>
  friend class ComputeProclet;

  void cleanup_steal();
};

}  // namespace nu

#include "nu/impl/task_range.ipp"

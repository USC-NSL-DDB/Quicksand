#include "sync.h"

#include "nu/utils/scoped_lock.hpp"

namespace nu {

template <class Impl>
TaskRange<Impl>::TaskRange(const TaskRange &o) {
  *this = o;
}

template <class Impl>
TaskRange<Impl> &TaskRange<Impl>::operator=(const TaskRange &o) {
  impl_ = o.impl_;
  size_ = o.size_;
  suspended_ = o.suspended_;
  return *this;
}

template <class Impl>
TaskRange<Impl>::TaskRange(TaskRange &&o) noexcept {
  *this = std::move(o);
}

template <class Impl>
TaskRange<Impl> &TaskRange<Impl>::operator=(TaskRange &&o) noexcept {
  impl_ = std::move(o.impl_);
  size_ = o.size_;
  suspended_ = o.suspended_;
  return *this;
}

template <class Impl>
TaskRange<Impl> TaskRange<Impl>::deep_copy() {
  TaskRange tr;

  if constexpr (DeepCopyAble<Impl>) {
    tr.impl_ = impl_.deep_copy();
  } else {
    tr.impl_ = impl_;
  }
  tr.size_ = size_;
  tr.suspended_ = suspended_;

  return tr;
}

template <class Impl>
TaskRange<Impl>::~TaskRange() {}

template <class Impl>
std::optional<typename Impl::Task> TaskRange<Impl>::pop() {
  {
    ScopedLock lock(&mutex_);

    if (unlikely(suspended_ || empty())) {
      if (suspended_) {
        while (rt::access_once(suspended_)) {
          suspend_cv_.wait(&mutex_);
        }
      }
      if (empty()) {
        return std::nullopt;
      }
    }

    size_--;
    processed_size_++;
  }

  return impl_.pop();
}

template <class Impl>
void TaskRange<Impl>::suspend() {
  ScopedLock lock(&mutex_);
  suspended_ = true;
  barrier();
  suspend_cv_.signal();
}

template <class Impl>
void TaskRange<Impl>::resume() {
  ScopedLock lock(&mutex_);
  __resume();
}

template <class Impl>
void TaskRange<Impl>::__resume() {
  suspended_ = false;
  barrier();
  suspend_cv_.signal();
}

template <class Impl>
Lazy<TaskRange<Impl>> TaskRange<Impl>::split(uint64_t last_n_elems) {
  if (unlikely(!last_n_elems)) {
    return make_lazy(TaskRange());
  }

  BUG_ON(size_ < last_n_elems);
  size_ -= last_n_elems;
  return transform_lazy(impl_.split(last_n_elems),
                        [](Impl impl) { return TaskRange(std::move(impl)); });
}

template <class Impl>
Lazy<TaskRange<Impl>> TaskRange<Impl>::steal() {
  ScopedLock lock(&mutex_);

  auto steal_size = size_ - size_ / 2;
  size_ /= 2;

  if (unlikely(empty())) {
    __resume();
  }

  return transform_lazy(impl_.split(steal_size),
                        [](Impl impl) { return TaskRange(std::move(impl)); });
}

template <class Impl>
TaskRangeIterator<Impl> TaskRange<Impl>::begin() const {
  return TaskRangeIterator(const_cast<TaskRange<Impl> &>(*this));
}

template <class Impl>
TaskRangeIterator<Impl> TaskRange<Impl>::end() const {
  return TaskRangeIterator(const_cast<TaskRange<Impl> &>(*this),
                           /* end = */ true);
}

template <class Impl>
TaskRangeIterator<Impl>::TaskRangeIterator(TaskRange<Impl> &range, bool end)
    : range_(range) {
  curr_ = (end || range_.empty()) ? std::nullopt : range_.pop();
}

template <class Impl>
inline bool TaskRangeIterator<Impl>::operator==(
    const TaskRangeIterator<Impl> &o) {
  return curr_ == o.curr_;
}

template <class Impl>
inline TaskRangeIterator<Impl>::Task TaskRangeIterator<Impl>::operator*()
    const {
  return std::move(*curr_);
}

template <class Impl>
inline TaskRangeIterator<Impl> &TaskRangeIterator<Impl>::operator++() {
  curr_ = range_.empty() ? std::nullopt : range_.pop();
  return *this;
}
}  // namespace nu

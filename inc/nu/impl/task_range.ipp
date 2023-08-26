#include "nu/utils/scoped_lock.hpp"
#include "sync.h"

namespace nu {

template <class Impl>
TaskRange<Impl>::TaskRange(const TaskRange &o) {
  *this = o;
}

template <class Impl>
TaskRange<Impl> &TaskRange<Impl>::operator=(const TaskRange &o) {
  impl_ = o.impl_;
  size_ = o.size_;
  cleared_ = o.cleared_;
  suspended_ = o.suspended_;
  BUG_ON(pending_steal_ || o.pending_steal_);
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
  cleared_ = o.cleared_;
  suspended_ = o.suspended_;
  BUG_ON(pending_steal_ || o.pending_steal_);
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
  tr.cleared_ = cleared_;
  tr.suspended_ = suspended_;
  BUG_ON(pending_steal_);

  return tr;
}

template <class Impl>
TaskRange<Impl>::~TaskRange() {
  BUG_ON(pending_steal_);
}

template <class Impl>
std::optional<typename Impl::Task> TaskRange<Impl>::pop() {
  if (unlikely(rt::access_once(pending_steal_) ||
               rt::access_once(suspended_))) {
    if (pending_steal_) {
      steal_size_ = size_ / 2;
      size_ -= steal_size_;
      pending_steal_ = false;
      barrier();
      cv_.signal();
    }
    if (suspended_) {
      ScopedLock lock(&mutex_);
      while (rt::access_once(suspended_)) {
        suspend_cv_.wait(&mutex_);
      }
    }
  }

  if (empty()) {
    return std::nullopt;
  }

  auto ret = impl_.pop();
  size_--;
  processed_size_++;
  return ret;
}

template <class Impl>
void TaskRange<Impl>::clear() {
  cleared_ = true;
  barrier();
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
void TaskRange<Impl>::cleanup_steal() {
  ScopedLock lock(&mutex_);
  pending_steal_ = false;
  barrier();
  cv_.signal();
}

template <class Impl>
TaskRange<Impl> TaskRange<Impl>::split(uint64_t last_n_elems) {
  if (unlikely(!last_n_elems)) {
    return TaskRange();
  }

  BUG_ON(size_ < last_n_elems);
  size_ -= last_n_elems;
  return impl_.split(last_n_elems);
}

template <class Impl>
TaskRange<Impl> TaskRange<Impl>::steal() {
  ScopedLock lock(&mutex_);
  if (suspended_) {
    auto steal_size = size_ < 2 ? size_ : size_ / 2;
    size_ -= steal_size;
    if (!size_) {
      __resume();
    }
    return impl_.split(steal_size);
  }

  if (unlikely(size() < 2)) {
    return TaskRange<Impl>();
  }
  pending_steal_ = true;
  while (rt::access_once(pending_steal_)) {
    cv_.wait(&mutex_);
  }
  auto steal_size = steal_size_;
  steal_size_ = 0;
  return impl_.split(steal_size);
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

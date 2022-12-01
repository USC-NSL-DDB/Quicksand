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
  BUG_ON(pending_steal_ || o.pending_steal_);
  return *this;
}

template <class Impl>
TaskRange<Impl>::~TaskRange() {
  BUG_ON(pending_steal_);
}

template <class Impl>
Impl::Task TaskRange<Impl>::pop() {
  if (unlikely(rt::access_once(pending_steal_))) {
    steal_size_ = size_ / 2;
    size_ -= steal_size_;
    pending_steal_ = false;
    barrier();
    cv_.signal();
  }

  auto ret = impl_.pop();
  size_--;
  return ret;
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
}  // namespace nu

#pragma once

#include <type_traits>

#include "nu/task_range.hpp"
#include "sharded_queue.hpp"

namespace nu {
template <typename T, BoolIntegral LL, BoolIntegral Insertable>
class QueueTaskRangeImpl;

template <class T>
concept QueueRangeBased = requires {
  requires is_base_of_template_v<T, QueueTaskRangeImpl>;
};

template <typename T, BoolIntegral LL>
class QueueInserter {
 public:
  QueueInserter(ShardedQueue<T, LL> &queue) : queue_(queue) {}
  void operator=(T &elem) { queue_.push(elem); }
  void operator=(T &&elem) { queue_.push(std::move(elem)); }

 private:
  ShardedQueue<T, LL> queue_;
};

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
class QueueTaskRangeImpl {
 public:
  using Key = std::size_t;
  using Task = std::conditional_t<Insertable::value, QueueInserter<T, LL>, T>;
  using Writeable = Insertable;

  QueueTaskRangeImpl();
  QueueTaskRangeImpl(ShardedQueue<T, LL> &queue);
  QueueTaskRangeImpl(const QueueTaskRangeImpl &) = default;
  QueueTaskRangeImpl &operator=(const QueueTaskRangeImpl &) = default;
  QueueTaskRangeImpl(QueueTaskRangeImpl &&) = default;
  QueueTaskRangeImpl &operator=(QueueTaskRangeImpl &&) = default;
  Task pop();
  QueueTaskRangeImpl split(uint64_t last_n_elems);
  Key l_key() const;
  std::size_t initial_size() const;
  std::size_t queue_length() const;
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  ShardedQueue<T, LL> queue_;
};

template <typename T, BoolIntegral LL>
using QueueRange =
    TaskRange<QueueTaskRangeImpl<T, LL, /* Insertable = */ std::false_type>>;

template <typename T, BoolIntegral LL>
using WriteableQueueRange =
    TaskRange<QueueTaskRangeImpl<T, LL, /* Insertable = */ std::true_type>>;

template <typename T, BoolIntegral LL>
QueueRange<T, LL> make_queue_range(ShardedQueue<T, LL> &queue);

template <typename T, BoolIntegral LL>
WriteableQueueRange<T, LL> make_writeable_queue_range(
    ShardedQueue<T, LL> &queue);

}  // namespace nu

#include "nu/impl/queue_range.ipp"

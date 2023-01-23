namespace nu {
template <typename T, BoolIntegral LL, BoolIntegral Insertable>
QueueTaskRangeImpl<T, LL, Insertable>::QueueTaskRangeImpl() {}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
QueueTaskRangeImpl<T, LL, Insertable>::QueueTaskRangeImpl(
    ShardedQueue<T, LL> &queue)
    : queue_(queue) {}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
QueueTaskRangeImpl<T, LL, Insertable>::Task
QueueTaskRangeImpl<T, LL, Insertable>::pop() {
  if constexpr (Insertable::value) {
    return QueueInserter(queue_);
  } else {
    return queue_.pop();
  }
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
QueueTaskRangeImpl<T, LL, Insertable>
QueueTaskRangeImpl<T, LL, Insertable>::split(uint64_t last_n_elems) {
  return *this;
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
QueueTaskRangeImpl<T, LL, Insertable>::Key
QueueTaskRangeImpl<T, LL, Insertable>::l_key() const {
  // FIXME: this is a temporary workaround to satisfy the current TaskRange API
  return 0;
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
std::size_t QueueTaskRangeImpl<T, LL, Insertable>::initial_size() const {
  // FIXME: this is a temporary workaround to satisfy the current TaskRange API
  return SIZE_MAX;
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
std::size_t QueueTaskRangeImpl<T, LL, Insertable>::queue_length() const {
  return queue_.size();
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
const ShardedQueue<T, LL> &QueueTaskRangeImpl<T, LL, Insertable>::queue()
    const {
  return queue_;
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
template <class Archive>
void QueueTaskRangeImpl<T, LL, Insertable>::save(Archive &ar) const {
  ar(queue_);
}

template <typename T, BoolIntegral LL, BoolIntegral Insertable>
template <class Archive>
void QueueTaskRangeImpl<T, LL, Insertable>::load(Archive &ar) {
  ar(queue_);
}

template <typename T, BoolIntegral LL>
QueueRange<T, LL> make_queue_range(ShardedQueue<T, LL> &queue) {
  return TaskRange(
      QueueTaskRangeImpl<T, LL, /* Insertable = */ std::false_type>(queue));
}

template <typename T, BoolIntegral LL>
WriteableQueueRange<T, LL> make_writeable_queue_range(
    ShardedQueue<T, LL> &queue) {
  return TaskRange(
      QueueTaskRangeImpl<T, LL, /* Insertable = */ std::true_type>(queue));
}

}  // namespace nu

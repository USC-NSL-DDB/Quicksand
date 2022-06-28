#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

namespace nu {
template <typename T>
ShardedVector<T> make_sharded_vector(uint32_t power_shard_sz,
                                     uint32_t remote_capacity,
                                     uint32_t max_write_batch_size) {
  uint32_t shard_max_size_bytes = (1 << power_shard_sz);
  BUG_ON(shard_max_size_bytes < sizeof(T));

  ShardedVector<T> vec;

  vec.power_shard_max_size_ = power_shard_sz;
  vec.shard_max_size_ = shard_max_size_bytes / sizeof(T);
  vec.max_tail_buffer_size_ = max_write_batch_size;
  vec.capacity_ = remote_capacity;

  size_t initial_shard_cnt =
      div_round_up_unchecked(vec.capacity_, (size_t)vec.shard_max_size_);

  if (initial_shard_cnt > 0) {
    for (size_t i = 0; i < initial_shard_cnt - 1; i++) {
      vec.shards_.emplace_back(
          make_proclet<typename nu::ShardedVector<T>::Shard<T>>(
              vec.shard_max_size_, vec.shard_max_size_));
    }
    size_t remaining_capacity =
        vec.capacity_ - (initial_shard_cnt - 1) * vec.shard_max_size_;
    vec.shards_.emplace_back(
        make_proclet<typename nu::ShardedVector<T>::Shard<T>>(
            remaining_capacity, vec.shard_max_size_));
  }

  vec.tail_buffer_.reserve(vec.max_tail_buffer_size_);

  return vec;
}

template <typename T>
ShardedVector<T>::ShardedVector()
    : shard_max_size_(0),
      power_shard_max_size_(0),
      size_(0),
      capacity_(0),
      max_tail_buffer_size_(0),
      tail_buffer_(0),
      buffered_shard_idx_((uint32_t)-1),
      read_buffer_(0),
      flush_reqs_(0),
      shards_(0) {}

template <typename T>
ShardedVector<T>::ShardedVector(const ShardedVector& o) {
  *this = o;
}

template <typename T>
ShardedVector<T>& ShardedVector<T>::operator=(const ShardedVector& o) {
  power_shard_max_size_ = o.power_shard_max_size_;
  shard_max_size_ = o.shard_max_size_;
  shards_ = o.shards_;
  size_ = o.size_;
  max_tail_buffer_size_ = o.max_tail_buffer_size_;
  tail_buffer_ = o.tail_buffer_;
  return *this;
}

template <typename T>
ShardedVector<T>::ShardedVector(ShardedVector&& o) {
  *this = std::move(o);
}

template <typename T>
ShardedVector<T>& ShardedVector<T>::operator=(ShardedVector&& o) {
  shard_max_size_ = o.shard_max_size_;
  power_shard_max_size_ = o.power_shard_max_size_;
  size_ = o.size_;
  shards_ = std::move(o.shards_);
  max_tail_buffer_size_ = o.max_tail_buffer_size_;
  tail_buffer_ = std::move(o.tail_buffer_);
  flush_reqs_ = std::move(o.flush_reqs_);
  return *this;
}

template <typename T>
T ShardedVector<T>::operator[](uint32_t index) {
  auto elem = calc_index(index);
  if (elem.in_buffer) {
    return tail_buffer_[elem.loc.buffer.idx];
  } else {
    if (unlikely(elem.loc.shard.shard_idx != buffered_shard_idx_)) {
      auto& shard = shards_[elem.loc.shard.shard_idx];
      auto shard_data = shard.run(&Shard<T>::collect);
      read_buffer_.clear();
      read_buffer_.insert(read_buffer_.end(), shard_data.begin(),
                          shard_data.end());
      buffered_shard_idx_ = elem.loc.shard.shard_idx;
    }
    return read_buffer_[elem.loc.shard.idx_in_shard];
  }
}

template <typename T>
void ShardedVector<T>::push_back_sync(const T& value) {
  push_back(value);
  flush();
}

template <typename T>
void ShardedVector<T>::push_back(const T& value) {
  BUG_ON(shard_max_size_ == 0);
  tail_buffer_.push_back(value);
  size_++;
  if (unlikely(tail_buffer_.size() >= max_tail_buffer_size_)) {
    init_flush();
  }
  _invalidate_read_buffer();
}

template <typename T>
void ShardedVector<T>::pop_back_sync() {
  pop_back();
  flush();
}

template <typename T>
void ShardedVector<T>::pop_back() {
  if (!tail_buffer_.empty()) {
    tail_buffer_.pop_back();
  } else {
    uint32_t shard_idx = (size_ - 1) / shard_max_size_;
    BUG_ON(shard_idx >= shards_.size());
    auto& shard = shards_[shard_idx];
    shard.run(&Shard<T>::pop_back);
    if (unlikely(shard_idx == buffered_shard_idx_)) {
      read_buffer_.pop_back();
    }
  }
  size_--;
}

template <typename T>
void ShardedVector<T>::flush() {
  init_flush();
  for (auto& future : flush_reqs_) {
    future.get();
  }
  flush_reqs_.clear();
}

template <typename T>
void ShardedVector<T>::init_flush() {
  if (tail_buffer_.empty()) return;

  for (auto& future : flush_reqs_) {
    future.get();
  }
  flush_reqs_.clear();

  BUG_ON(size_ < tail_buffer_.size());
  size_t synced_sz_ = size_ - tail_buffer_.size();
  size_t start = 0, to_send = 0;
  while (start < tail_buffer_.size()) {
    size_t remaining = tail_buffer_.size() - start;
    size_t shard_idx = synced_sz_ / shard_max_size_;

    if (shard_idx < shards_.size()) {
      size_t shard_remaining_cap =
          shard_max_size_ - (synced_sz_ % shard_max_size_);
      to_send = std::min(remaining, shard_remaining_cap);

      std::vector<T> elems(
          std::make_move_iterator(tail_buffer_.begin() + start),
          std::make_move_iterator(tail_buffer_.begin() + start + to_send));

      flush_reqs_.emplace_back(shards_[shard_idx].run_async(
          &Shard<T>::push_back_batch, std::move(elems)));
    } else {
      to_send = std::min(remaining, (size_t)shard_max_size_);
      std::vector<T> elems(
          std::make_move_iterator(tail_buffer_.begin() + start),
          std::make_move_iterator(tail_buffer_.begin() + start + to_send));
      shards_.emplace_back(
          make_proclet<Shard<T>>(std::move(elems), shard_max_size_));
    }

    start += to_send;
    synced_sz_ += to_send;
  }
  tail_buffer_.clear();
}

template <typename T>
template <typename T1>
void ShardedVector<T>::set(uint32_t index, T1&& value) {
  if (index >= size_) return;
  ElemIndex elem = calc_index(index);
  if (elem.in_buffer) {
    tail_buffer_[elem.loc.buffer.idx] = value;
  } else {
    shards_[elem.loc.shard.shard_idx].run(
        +[](Shard<T>& shard, uint32_t idx, T value) { shard.set(idx, value); },
        elem.loc.shard.idx_in_shard, value);
    if (unlikely(elem.loc.shard.shard_idx == buffered_shard_idx_)) {
      read_buffer_[elem.loc.shard.idx_in_shard] = value;
    }
  }
}

template <typename T>
template <typename... A0s, typename... A1s>
void ShardedVector<T>::apply(uint32_t index, void (*fn)(T&, A0s...),
                             A1s&&... args) {
  return apply_async(index, fn, std::forward(args)...).get();
}

template <typename T>
template <typename... A0s, typename... A1s>
Future<void> ShardedVector<T>::apply_async(uint32_t index,
                                           void (*fn)(T&, A0s...),
                                           A1s&&... args) {
  Future<void> noop;
  if (index >= size_) return noop;
  using Fn = decltype(fn);
  ElemIndex elem = calc_index(index);
  if (elem.in_buffer) {
    fn(tail_buffer_[elem.loc.buffer.idx], std::forward(args)...);
    return noop;
  } else {
    if (unlikely(elem.loc.shard.shard_idx == buffered_shard_idx_)) {
      _invalidate_read_buffer();
    }
    return shards_[elem.loc.shard.shard_idx].__run_async(
        +[](Shard<T>& shard, uint32_t idx, Fn fn, A1s&&... args) {
          shard.apply(idx, fn, std::forward(args)...);
        },
        elem.loc.shard.idx_in_shard, fn, std::forward(args)...);
  }
}

template <typename T>
constexpr bool ShardedVector<T>::empty() const noexcept {
  return size_ == 0;
}

template <typename T>
constexpr size_t ShardedVector<T>::size() const noexcept {
  return size_;
}

template <typename T>
void ShardedVector<T>::clear() {
  size_ = 0;
  tail_buffer_.clear();
  _invalidate_read_buffer();
  std::vector<Future<void>> futures;
  for (uint32_t i = 0; i < shards_.size(); i++) {
    futures.emplace_back(shards_[i].run_async(&Shard<T>::clear));
  }
  for (auto& future : futures) {
    future.get();
  }
}

template <typename T>
size_t ShardedVector<T>::capacity() {
  std::vector<size_t> capacities =
      __for_all_shards(+[](Shard<T>& shard) { return shard.capacity(); });
  return std::accumulate(capacities.begin(), capacities.end(), 0);
}

template <typename T>
void ShardedVector<T>::shrink_to_fit() {
  if (capacity_ < size_) {
    // capacity_ can be stale, so it can appear to be lower than size_.
    // to keep shrink_to_fit() cheap, do not query all shards to get
    // the vector's actual capacity.
    return;
  }

  size_t num_shards = size_ / shard_max_size_ + 1 * (size_ % shard_max_size_);
  BUG_ON(num_shards > shards_.size());
  shards_.resize(num_shards);
}

template <typename T>
void ShardedVector<T>::reserve(size_t new_cap) {
  size_t cur_cap = capacity();
  if (new_cap <= cur_cap) return;

  size_t last_shard_cap = cur_cap % shard_max_size_;
  if (last_shard_cap != 0) {
    shards_.back().run(
        +[](Shard<T>& shard, size_t cap) { shard.reserve(cap); },
        shard_max_size_);
    cur_cap += (shard_max_size_ - last_shard_cap);
  }
  while (cur_cap < new_cap) {
    size_t shard_cap = shard_max_size_;
    shards_.emplace_back(make_proclet<Shard<T>>(shard_cap, shard_max_size_));
    cur_cap += shard_max_size_;
  }
  capacity_ = cur_cap;
}

template <typename T>
void ShardedVector<T>::resize(size_t count) {
  if (count == size_) return;

  if (count < size_) {
    _resize_down(count);
  } else {
    _resize_up(count);
  }

  _invalidate_read_buffer();
}

template <typename T>
void ShardedVector<T>::_resize_down(size_t target_size) {
  BUG_ON(!(target_size < size_));
  BUG_ON(shards_.empty());

  size_t cur_size = size_;
  size_t last_shard_size = size_ % shard_max_size_;
  if (last_shard_size != 0) {
    size_t truncated =
        std::min((cur_size - target_size), (size_t)last_shard_size);
    size_t size_target = last_shard_size - truncated;
    shards_.back().run(
        +[](Shard<T>& shard, size_t target) { shard.resize(target); },
        size_target);
    cur_size -= truncated;
  }

  auto shard = shards_.rbegin()++;
  while (cur_size > target_size && shard != shards_.rend()) {
    size_t truncated =
        std::min((cur_size - target_size), (size_t)shard_max_size_);
    size_t size_target = shard_max_size_ - truncated;
    (*shard).run(
        +[](Shard<T>& shard, size_t target) { shard.resize(target); },
        size_target);
    cur_size -= truncated;
    shard++;
  }

  size_ = cur_size;
}

template <typename T>
void ShardedVector<T>::_resize_up(size_t target_size) {
  BUG_ON(!(target_size > size_));

  size_t cur_size = size_;
  size_t last_shard_size = size_ % shard_max_size_;
  if (last_shard_size != 0) {
    size_t extended = std::min((target_size - cur_size),
                               (size_t)(shard_max_size_ - last_shard_size));
    size_t size_target = last_shard_size + extended;
    shards_.back().run(
        +[](Shard<T>& shard, size_t target) { shard.resize(target); },
        size_target);
    cur_size += extended;
  }

  while (cur_size < target_size) {
    size_t shard_sz =
        std::min((target_size - cur_size), (size_t)shard_max_size_);
    size_t capacity = shard_max_size_;
    shards_.emplace_back(
        make_proclet<Shard<T>>(capacity, shard_max_size_, shard_sz));
    cur_size += shard_sz;
  }

  size_ = cur_size;
}

template <typename T>
template <typename... A0s, typename... A1s>
ShardedVector<T>& ShardedVector<T>::for_all(T (*fn)(T, A0s...), A1s&&... args) {
  flush();
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  __for_all_shards(
      +[](Shard<T>& shard, uintptr_t raw_fn, A1s&&... args) {
        auto* fn = reinterpret_cast<Fn>(raw_fn);
        shard.for_all(fn, args...);
      },
      raw_fn, args...);
  _invalidate_read_buffer();
  return *this;
}

template <typename T>
template <typename... A0s, typename... A1s>
ShardedVector<T>& ShardedVector<T>::for_all(void (*fn)(T&, A0s...),
                                            A1s&&... args) {
  flush();
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  __for_all_shards(
      +[](Shard<T>& shard, uintptr_t raw_fn, A1s&&... args) {
        auto* fn = reinterpret_cast<Fn>(raw_fn);
        shard.for_all(fn, args...);
      },
      raw_fn, args...);
  _invalidate_read_buffer();
  return *this;
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedVector<T>::reduce(RetT initial_val,
                              RetT (*reducer)(RetT, T, A0s...), A1s&&... args) {
  flush();
  if (shards_.size() == 0) return initial_val;

  using Fn = decltype(reducer);
  auto results = __for_all_shards(
      +[](Shard<T>& shard, Fn fn, RetT initial_val, A1s&&... args) {
        return shard.reduce(initial_val, fn, args...);
      },
      reducer, initial_val, std::forward<A1s>(args)...);

  BUG_ON(results.size() == 0);
  RetT output = results[0];
  for (uint32_t i = 1; i < results.size(); i++) {
    output = reducer(output, results[i], args...);
  }
  return output;
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedVector<T>::reduce(RetT initial_val,
                              void (*reducer)(RetT&, T&, A0s...),
                              A1s&&... args) {
  flush();
  if (shards_.size() == 0) return initial_val;

  using Fn = decltype(reducer);
  RetT out = std::move(initial_val);
  for (uint32_t i = 0; i < shards_.size(); i++) {
    out = shards_[i].__run(
        +[](Shard<T>& shard, RetT initial_val, Fn reducer, A1s&&... args) {
          return shard.reduce(initial_val, reducer, std::forward(args)...);
        },
        std::move(out), reducer, std::forward(args)...);
  }

  return out;
}

template <typename T>
template <typename T1, typename... A0s, typename... A1s>
ShardedVector<T1> ShardedVector<T>::map(T1 (*fn)(T, A0s...), A1s&&... args) {
  using Fn = decltype(fn);

  flush();
  auto vec_new = make_sharded_vector<T1>(power_shard_max_size_);
  vec_new.shards_ = __for_all_shards(
      +[](Shard<T>& shard, Fn fn, A1s&&... args) {
        return make_proclet<Shard<T1>>(
            shard.map(fn, std::forward<A1s>(args)...));
      },
      fn, std::forward(args)...);
  vec_new.size_ = size_;

  return vec_new;
}

template <typename T>
inline ShardedVector<T>::ElemIndex ShardedVector<T>::calc_index(
    uint32_t index) {
  BUG_ON(size_ < tail_buffer_.size());
  size_t synced_sz = size_ - tail_buffer_.size();
  ElemIndex out;
  out.in_buffer = index >= synced_sz;
  if (out.in_buffer) {
    out.loc.buffer.idx = index - synced_sz;
  } else {
    out.loc.shard.shard_idx = index / shard_max_size_;
    out.loc.shard.idx_in_shard = index % shard_max_size_;
  }
  return out;
}

template <typename T>
std::vector<T> ShardedVector<T>::collect() {
  std::vector<std::vector<T>> shard_data =
      __for_all_shards(+[](Shard<T>& shard) { return shard.collect(); });

  std::vector<T> output;
  for (auto& data : shard_data) {
    output.insert(output.end(), data.begin(), data.end());
  }

  return output;
}

template <typename T>
template <class Archive>
void ShardedVector<T>::serialize(Archive& ar) {
  ar(power_shard_max_size_);
  ar(shard_max_size_);
  ar(size_);
  ar(capacity_);
  ar(max_tail_buffer_size_);
  ar(tail_buffer_);
  ar(buffered_shard_idx_);
  ar(read_buffer_);
  ar(flush_reqs_);
  ar(shards_);
}

template <typename T>
inline void ShardedVector<T>::_invalidate_read_buffer() {
  buffered_shard_idx_ = (uint32_t)-1;
}

template <typename T>
template <typename V, typename... A0s, typename... A1s>
std::vector<V> ShardedVector<T>::__for_all_shards(V (*fn)(Shard<T>&, A0s...),
                                                  A1s&&... args) {
  std::vector<V> out;
  out.reserve(shards_.size());
  std::vector<Future<V>> futures;
  futures.reserve(shards_.size());

  for (uint32_t i = 0; i < shards_.size(); i++) {
    futures.emplace_back(
        shards_[i].__run_async(fn, std::forward<A1s>(args)...));
  }
  for (auto& future : futures) {
    out.emplace_back(future.get());
  }

  return out;
}

template <typename T>
template <typename... A0s, typename... A1s>
void ShardedVector<T>::__for_all_shards(void (*fn)(Shard<T>&, A0s...),
                                        A1s&&... args) {
  std::vector<Future<void>> futures;
  for (uint32_t i = 0; i < shards_.size(); i++) {
    futures.emplace_back(
        shards_[i].__run_async(fn, std::forward<A1s>(args)...));
  }
  for (auto& future : futures) {
    future.get();
  }
}

template <typename T>
template <typename T1>
ShardedVector<T>::Shard<T1>::Shard() : data_(0), size_max_(0) {}

template <typename T>
template <typename T1>
ShardedVector<T>::Shard<T1>::Shard(size_t capacity, uint32_t size_max,
                                   uint32_t initial_size)
    : data_(0), size_max_(size_max) {
  if (capacity) {
    data_.reserve(capacity);
  }
  if (initial_size) {
    data_.resize(initial_size);
  }
}

template <typename T>
template <typename T1>
ShardedVector<T>::Shard<T1>::Shard(std::vector<T1> elems, uint32_t size_max) {
  BUG_ON(size_max < elems.size());
  size_max_ = size_max;
  data_ = std::move(elems);
}

template <typename T>
template <typename T1>
T1 ShardedVector<T>::Shard<T1>::operator[](uint32_t index) {
  return data_[index];
}

template <typename T>
template <typename T1>
void ShardedVector<T>::Shard<T1>::push_back(const T1& value) {
  BUG_ON(data_.size() > size_max_);
  data_.push_back(value);
}

template <typename T>
template <typename T1>
void ShardedVector<T>::Shard<T1>::push_back_batch(std::vector<T1> elems) {
  BUG_ON(data_.size() + elems.size() > size_max_);
  data_.insert(data_.end(), elems.begin(), elems.end());
}

template <typename T>
template <typename T1>
void ShardedVector<T>::Shard<T1>::pop_back() {
  BUG_ON(data_.size() == 0);
  data_.pop_back();
}

template <typename T>
template <typename T1>
template <typename T2>
void ShardedVector<T>::Shard<T1>::set(uint32_t index, T2&& value) {
  data_[index] = value;
}

template <typename T>
template <typename T1>
template <typename... A0s, typename... A1s>
void ShardedVector<T>::Shard<T1>::apply(uint32_t index, void (*fn)(T1&, A0s...),
                                        A1s&&... args) {
  T& elem = data_[index];
  fn(elem, std::forward(args)...);
}

template <typename T>
template <typename T1>
void ShardedVector<T>::Shard<T1>::clear() {
  data_.clear();
}

template <typename T>
template <typename T1>
size_t ShardedVector<T>::Shard<T1>::capacity() const {
  return data_.capacity();
}

template <typename T>
template <typename T1>
void ShardedVector<T>::Shard<T1>::reserve(size_t new_cap) {
  data_.reserve(new_cap);
}

template <typename T>
template <typename T1>
void ShardedVector<T>::Shard<T1>::resize(size_t count) {
  data_.resize(count);
}

template <typename T>
template <typename T1>
std::vector<T1> ShardedVector<T>::Shard<T1>::collect() {
  return data_;
}

template <typename T>
template <typename T1>
template <typename... A0s, typename... A1s>
void ShardedVector<T>::Shard<T1>::for_all(T1 (*fn)(T1, A0s...), A1s&&... args) {
  std::transform(data_.cbegin(), data_.cend(), data_.begin(),
                 [=](T elem) { return fn(elem, args...); });
}

template <typename T>
template <typename T1>
template <typename... A0s, typename... A1s>
void ShardedVector<T>::Shard<T1>::for_all(void (*fn)(T1&, A0s...),
                                          A1s&&... args) {
  std::for_each(data_.begin(), data_.end(),
                [=](T& elem) { fn(elem, args...); });
}

template <typename T>
template <typename T1>
template <typename T2, typename... A0s, typename... A1s>
ShardedVector<T>::Shard<T2> ShardedVector<T>::Shard<T1>::map(T2 (*fn)(T1,
                                                                      A0s...),
                                                             A1s&&... args) {
  std::vector<T2> mapped;
  mapped.reserve(data_.size());
  std::transform(data_.begin(), data_.end(), std::back_inserter(mapped),
                 [=](T1 elem) { return fn(elem, std::forward<A1s>(args)...); });
  return Shard(std::move(mapped), size_max_);
}

template <typename T>
template <typename T1>
template <typename T2, typename... A0s, typename... A1s>
ShardedVector<T>::Shard<T2> ShardedVector<T>::Shard<T1>::map(T2 (*fn)(const T1&,
                                                                      A0s...),
                                                             A1s&&... args) {
  std::vector<T2> mapped;
  mapped.reserve(data_.size());
  std::transform(data_.cbegin(), data_.cend(), std::back_inserter(mapped),
                 [=](const T1& elem) {
                   return fn(std::forward(elem), std::forward<A1s>(args)...);
                 });
  return Shard(std::move(mapped), size_max_);
}

template <typename T>
template <typename T1>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedVector<T>::Shard<T1>::reduce(RetT initial_val,
                                         RetT (*reducer)(RetT, T1, A0s...),
                                         A1s&&... args) {
  return std::reduce(
      data_.cbegin(), data_.cend(), initial_val,
      [=](T elem, RetT acc) { return reducer(acc, elem, args...); });
}

template <typename T>
template <typename T1>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedVector<T>::Shard<T1>::reduce(RetT initial_val,
                                         void (*reducer)(RetT&, T1&, A0s...),
                                         A1s&&... args) {
  RetT out = std::move(initial_val);
  for (size_t i = 0; i < data_.size(); i++) {
    reducer(out, data_[i], std::forward(args)...);
  }
  return out;
}

template <typename T>
template <typename T1>
template <class Archive>
void ShardedVector<T>::Shard<T1>::serialize(Archive& ar) {
  ar(data_);
  ar(size_max_);
}

}  // namespace nu

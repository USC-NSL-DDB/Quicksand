#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>

#include "nu/commons.hpp"

#define DIV_ROUND_UP_UNCHECKED(dividend, divisor) \
  (dividend + divisor - 1) / divisor

namespace nu {
template <typename T>
ElRef<T>::ElRef() {}

template <typename T>
ElRef<T>::ElRef(uint32_t index, T element) : el_(element), idx_(index) {}

template <typename T>
const T& ElRef<T>::operator*() {
  return el_;
}

template <typename T>
template <typename T1>
bool ElRef<T>::operator==(T1&& rhs) {
  return rhs == el_;
}

template <typename T>
ElRef<T>& ElRef<T>::operator=(const T& value) {
  shard_.value().run(
      +[](VectorShard<T>& shard, uint32_t idx, T value) {
        shard.data_[idx] = value;
      },
      idx_, value);
  return *this;
}

template <typename T>
template <class Archive>
void ElRef<T>::serialize(Archive& ar) {
  ar(el_);
  ar(idx_);
  ar(shard_);
}

template <typename T>
VectorShard<T>::VectorShard() : data_(0), size_max_(0) {}

template <typename T>
VectorShard<T>::VectorShard(size_t capacity, uint32_t size_max)
    : data_(0), size_max_(size_max) {
  if (capacity) {
    data_.reserve(capacity);
  }
}

template <typename T>
ElRef<T> VectorShard<T>::operator[](uint32_t index) {
  return ElRef(index, data_[index]);
}

template <typename T>
void VectorShard<T>::push_back(const T& value) {
  BUG_ON(data_.size() > size_max_);
  data_.push_back(value);
}

template <typename T>
void VectorShard<T>::pop_back() {
  BUG_ON(data_.size() == 0);
  data_.pop_back();
}

template <typename T>
void VectorShard<T>::clear() {
  data_.clear();
}

template <typename T>
size_t VectorShard<T>::capacity() const {
  return data_.capacity();
}

template <typename T>
void VectorShard<T>::reserve(size_t new_cap) {
  data_.reserve(new_cap);
}

template <typename T>
void VectorShard<T>::resize(size_t count) {
  data_.resize(count);
}

template <typename T>
template <typename... A0s, typename... A1s>
void VectorShard<T>::transform(T (*fn)(T, A0s...), A1s&&... args) {
  std::transform(data_.cbegin(), data_.cend(), data_.begin(),
                 [=](T elem) { return fn(elem, args...); });
}

template <typename T>
template <typename... A0s, typename... A1s>
void VectorShard<T>::transform(void (*fn)(T&, A0s...), A1s&&... args) {
  std::for_each(data_.begin(), data_.end(),
                [=](T& elem) { fn(elem, args...); });
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT VectorShard<T>::reduce(RetT initial_val, RetT (*reducer)(RetT, T, A0s...),
                            A1s&&... args) {
  return std::reduce(
      data_.cbegin(), data_.cend(), initial_val,
      [=](T elem, RetT acc) { return reducer(acc, elem, args...); });
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT VectorShard<T>::reduce(RetT initial_val,
                            void (*reducer)(RetT&, T&, A0s...), A1s&&... args) {
  RetT out = std::move(initial_val);
  for (size_t i = 0; i < data_.size(); i++) {
    reducer(out, data_[i], std::forward(args)...);
  }
  return out;
}

template <typename T>
ShardedVector<T>::ShardedVector()
    : shard_max_size_(0),
      shard_max_size_bytes_(0),
      size_(0),
      capacity_(0),
      shards_(0) {}

template <typename T>
ShardedVector<T>::ShardedVector(const ShardedVector& o) {
  *this = o;
}

template <typename T>
ShardedVector<T>& ShardedVector<T>::operator=(const ShardedVector& o) {
  shard_max_size_bytes_ = o.shard_max_size_bytes_;
  shard_max_size_ = o.shard_max_size_;
  shards_ = o.shards_;
  size_ = o.size_;
  return *this;
}

template <typename T>
ShardedVector<T>::ShardedVector(ShardedVector&& o) {
  *this = std::move(o);
}

template <typename T>
ShardedVector<T>& ShardedVector<T>::operator=(ShardedVector&& o) {
  shard_max_size_ = o.shard_max_size_;
  shard_max_size_bytes_ = o.shard_max_size_bytes_;
  size_ = o.size_;
  for (uint32_t i = 0; i < o.shards_.size(); i++) {
    shards_.emplace_back(std::move(o.shards_[i]));
  }
  return *this;
}

template <typename T>
ElRef<T> ShardedVector<T>::operator[](uint32_t index) {
  uint32_t shard_idx = index / shard_max_size_;
  uint32_t idx_in_shard = index % shard_max_size_;
  auto& shard = shards_[shard_idx];
  auto ret = shard.__run(
      +[](VectorShard<T>& shard, uint32_t idx) { return shard[idx]; },
      idx_in_shard);
  ret.shard_ = shard.get_weak();
  return ret;
}

template <typename T>
void ShardedVector<T>::push_back(const T& value) {
  BUG_ON(shard_max_size_ == 0);
  uint32_t shard_idx = size_ / shard_max_size_;
  BUG_ON(shard_idx > shards_.size());
  if (shard_idx == shards_.size()) {
    uint32_t capacity = shard_max_size_;
    uint32_t max_size = shard_max_size_;
    shards_.emplace_back(make_proclet<VectorShard<T>>(capacity, max_size));
  }
  auto& shard = shards_[shard_idx];
  shard.__run(
      +[](VectorShard<T>& shard, T value) { shard.push_back(value); }, value);
  size_++;
}

template <typename T>
void ShardedVector<T>::pop_back() {
  if (size_ == 0) return;

  BUG_ON(shard_max_size_ == 0);
  uint32_t shard_idx = (size_ - 1) / shard_max_size_;
  BUG_ON(shard_idx >= shards_.size());

  bool last_shard_empty = ((size_ - 1) % shard_max_size_) == 0;
  if (last_shard_empty) {
    shards_.pop_back();
  } else {
    auto& shard = shards_[shard_idx];
    shard.__run(+[](VectorShard<T>& shard) { shard.pop_back(); });
  }
  size_--;
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
  std::vector<Future<void>> futures;
  for (uint32_t i = 0; i < shards_.size(); i++) {
    futures.emplace_back(shards_[i].__run_async(
        +[](VectorShard<T>& shard) { return shard.clear(); }));
  }
  for (auto& future : futures) {
    future.get();
  }
}

template <typename T>
size_t ShardedVector<T>::capacity() {
  std::vector<size_t> capacities =
      __for_all_shards(+[](VectorShard<T>& shard) { return shard.capacity(); });
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
    shards_.back().__run(
        +[](VectorShard<T>& shard, size_t cap) { shard.reserve(cap); },
        shard_max_size_);
    cur_cap += (shard_max_size_ - last_shard_cap);
  }
  while (cur_cap < new_cap) {
    size_t shard_cap = shard_max_size_;
    shards_.emplace_back(
        make_proclet<VectorShard<T>>(shard_cap, shard_max_size_));
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
    shards_.back().__run(
        +[](VectorShard<T>& shard, size_t target) { shard.resize(target); },
        size_target);
    cur_size -= truncated;
  }

  auto shard = shards_.rbegin()++;
  while (cur_size > target_size && shard != shards_.rend()) {
    size_t truncated =
        std::min((cur_size - target_size), (size_t)shard_max_size_);
    size_t size_target = shard_max_size_ - truncated;
    (*shard).__run(
        +[](VectorShard<T>& shard, size_t target) { shard.resize(target); },
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
    shards_.back().__run(
        +[](VectorShard<T>& shard, size_t target) { shard.resize(target); },
        size_target);
    cur_size += extended;
  }

  while (cur_size < target_size) {
    size_t shard_sz =
        std::min((target_size - cur_size), (size_t)shard_max_size_);
    shards_.emplace_back(
        make_proclet<VectorShard<T>>(shard_sz, shard_max_size_));
    cur_size += shard_sz;
  }

  size_ = cur_size;
}

template <typename T>
template <typename... A0s, typename... A1s>
ShardedVector<T>& ShardedVector<T>::transform(T (*fn)(T, A0s...),
                                              A1s&&... args) {
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  __for_all_shards(
      +[](VectorShard<T>& shard, uintptr_t raw_fn, A1s&&... args) {
        auto* fn = reinterpret_cast<Fn>(raw_fn);
        shard.transform(fn, args...);
      },
      raw_fn, args...);
  return *this;
}

template <typename T>
template <typename... A0s, typename... A1s>
ShardedVector<T>& ShardedVector<T>::transform(void (*fn)(T&, A0s...),
                                              A1s&&... args) {
  using Fn = decltype(fn);
  auto raw_fn = reinterpret_cast<uintptr_t>(fn);
  __for_all_shards(
      +[](VectorShard<T>& shard, uintptr_t raw_fn, A1s&&... args) {
        auto* fn = reinterpret_cast<Fn>(raw_fn);
        shard.transform(fn, args...);
      },
      raw_fn, args...);
  return *this;
}

template <typename T>
template <typename RetT, typename... A0s, typename... A1s>
RetT ShardedVector<T>::reduce(RetT initial_val,
                              RetT (*reducer)(RetT, T, A0s...), A1s&&... args) {
  if (shards_.size() == 0) return initial_val;

  using Fn = decltype(reducer);
  auto results = __for_all_shards(
      +[](VectorShard<T>& shard, Fn fn, RetT initial_val, A1s&&... args) {
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
  if (shards_.size() == 0) return initial_val;

  using Fn = decltype(reducer);
  RetT out = std::move(initial_val);
  for (uint32_t i = 0; i < shards_.size(); i++) {
    out = shards_[i].__run(
        +[](VectorShard<T>& shard, RetT initial_val, Fn reducer,
            A1s&&... args) {
          return shard.reduce(initial_val, reducer, std::forward(args)...);
        },
        std::move(out), reducer, std::forward(args)...);
  }

  return out;
}

template <typename T>
std::vector<T> ShardedVector<T>::collect() {
  std::vector<std::vector<T>> shard_data =
      __for_all_shards(+[](VectorShard<T>& shard) { return shard.data_; });

  std::vector<T> output;
  for (auto& data : shard_data) {
    output.insert(output.end(), data.begin(), data.end());
  }

  return output;
}

template <typename T>
template <class Archive>
void ShardedVector<T>::serialize(Archive& ar) {
  ar(shard_max_size_bytes_);
  ar(shard_max_size_);
  ar(size_);
  ar(capacity_);
  ar(shards_);
}

template <typename T>
template <typename V, typename... A0s, typename... A1s>
std::vector<V> ShardedVector<T>::__for_all_shards(V (*fn)(VectorShard<T>&,
                                                          A0s...),
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
void ShardedVector<T>::__for_all_shards(void (*fn)(VectorShard<T>&, A0s...),
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
ShardedVector<T> make_dis_vector(uint32_t power_shard_sz, size_t capacity) {
  ShardedVector<T> vec;

  vec.shard_max_size_bytes_ = (1 << power_shard_sz);
  vec.shard_max_size_ = vec.shard_max_size_bytes_ / sizeof(T);
  vec.capacity_ = capacity;

  BUG_ON(vec.shard_max_size_bytes_ < sizeof(T));

  size_t initial_shard_cnt =
      DIV_ROUND_UP_UNCHECKED(vec.capacity_, vec.shard_max_size_);

  if (initial_shard_cnt > 0) {
    for (size_t i = 0; i < initial_shard_cnt - 1; i++) {
      vec.shards_.emplace_back(make_proclet<VectorShard<T>>(
          vec.shard_max_size_, vec.shard_max_size_));
    }
    size_t remaining_capacity =
        vec.capacity_ - (initial_shard_cnt - 1) * vec.shard_max_size_;
    vec.shards_.emplace_back(
        make_proclet<VectorShard<T>>(remaining_capacity, vec.shard_max_size_));
  }

  return vec;
}
}  // namespace nu

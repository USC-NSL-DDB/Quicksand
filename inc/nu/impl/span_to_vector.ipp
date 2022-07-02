extern "C" {
#include <base/assert.h>
}

#include <cereal/archives/binary.hpp>

namespace nu {

template <typename T>
SpanToVectorWrapper<T>::SpanToVectorWrapper() {}

template <typename T>
SpanToVectorWrapper<T>::SpanToVectorWrapper(std::span<T> s)
    : s_(s), vec_capacity_(0) {}

template <typename T>
SpanToVectorWrapper<T>::SpanToVectorWrapper(std::span<T> s,
                                            uint32_t vec_capacity)
    : s_(s), vec_capacity_(vec_capacity) {}

template <typename T>
SpanToVectorWrapper<T>::SpanToVectorWrapper(const SpanToVectorWrapper &o) {
  *this = o;
}

template <typename T>
SpanToVectorWrapper<T> &SpanToVectorWrapper<T>::operator=(
    const SpanToVectorWrapper &o) {
  BUG_ON(o.s_.empty());
  v_.reserve(o.vec_capacity_);
  v_.assign(o.s_.begin(), o.s_.end());
  return *this;
}

template <typename T>
SpanToVectorWrapper<T>::SpanToVectorWrapper(SpanToVectorWrapper &&o) noexcept
    : s_(std::move(o.s_)),
      v_(std::move(o.v_)),
      vec_capacity_(o.vec_capacity_) {}

template <typename T>
SpanToVectorWrapper<T> &SpanToVectorWrapper<T>::operator=(
    SpanToVectorWrapper &&o) noexcept {
  s_ = std::move(o.s_);
  v_ = std::move(o.v_);
  vec_capacity_ = o.vec_capacity_;
  return *this;
}

template <typename T>
std::vector<T> &SpanToVectorWrapper<T>::unwrap() {
  BUG_ON(!s_.empty());
  return v_;
}

template <typename T>
template <class Archive>
void SpanToVectorWrapper<T>::save(Archive &ar) const {
  BUG_ON(s_.empty());

  ar(vec_capacity_);
  std::size_t size = s_.size();
  ar(size);

  if constexpr (std::is_same<T, bool>::value) {
    for (const auto t : s_) {
      ar(t);
    }
  } else if constexpr (!std::is_arithmetic<T>::value) {
    for (const auto &t : s_) {
      ar(t);
    }
  } else {
    ar(cereal::binary_data(s_.data(), s_.size_bytes()));
  }
}

template <typename T>
template <class Archive>
void SpanToVectorWrapper<T>::load(Archive &ar) {
  ar(vec_capacity_);
  v_.reserve(vec_capacity_);

  std::size_t size;
  ar(size);
  v_.resize(size);

  if constexpr (std::is_same<T, bool>::value) {
    bool b;
    for (auto t : v_) {
      ar(b);
      t = b;
    }
  } else if constexpr (!std::is_arithmetic<T>::value) {
    for (auto &t : v_) {
      ar(t);
    }
  } else {
    ar(cereal::binary_data(v_.data(), size * sizeof(T)));
  }
}

}  // namespace nu

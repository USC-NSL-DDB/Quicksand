#pragma once

#include <span>
#include <vector>

namespace nu {

template <typename T>
class SpanToVectorWrapper {
 public:
  SpanToVectorWrapper();
  SpanToVectorWrapper(std::span<T> s);
  SpanToVectorWrapper(std::span<T> s, uint32_t vec_capacity);
  SpanToVectorWrapper(const SpanToVectorWrapper &);
  SpanToVectorWrapper &operator=(const SpanToVectorWrapper &);
  SpanToVectorWrapper(SpanToVectorWrapper &&) noexcept;
  SpanToVectorWrapper &operator=(SpanToVectorWrapper &&) noexcept;
  std::vector<T> &unwrap();
  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  std::span<T> s_;
  std::vector<T> v_;
  uint32_t vec_capacity_;
};

}  // namespace nu

#include "nu/impl/span_to_vector.ipp"

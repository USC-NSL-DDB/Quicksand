#pragma once

#include <functional>
#include <optional>

namespace nu {

template <typename T>
class Lazy {
 public:
  T &get();
  bool is_evaluated() const;

 private:
  Lazy(std::function<T()> &&fn);
  template <typename U>
  friend Lazy<U> make_lazy(std::function<U()>);

  std::function<T()> fn_;
  std::optional<T> t_;
};

template <typename T>
Lazy<T> make_lazy(std::function<T()> fn);

}  // namespace nu

#include "nu/impl/lazy.ipp"

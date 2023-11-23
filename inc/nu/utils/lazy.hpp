#pragma once

#include <functional>
#include <optional>
#include <type_traits>

namespace nu {

template <typename T>
class Lazy {
 public:
  T &get();
  bool is_evaluated() const;

 private:
  Lazy(std::move_only_function<T()> &&fn);
  Lazy(T &&t);
  template <typename U>
  friend Lazy<U> make_lazy(U);
  template <typename U, typename F>
  friend Lazy<U> make_lazy(F &&)
    requires(!std::is_same_v<U, F>);
  template <typename U, typename F>
  friend auto transform_lazy(Lazy<U> &&, F &&) -> Lazy<std::result_of_t<F(U)>>;

  std::move_only_function<T()> fn_;
  std::optional<T> t_;
};

template <typename T>
Lazy<T> make_lazy(T t);

template <typename T, typename F>
Lazy<T> make_lazy(F &&f)
  requires(!std::is_same_v<T, F>);

template <typename T, typename F>
auto transform_lazy(Lazy<T> &&u, F &&f) -> Lazy<std::result_of_t<F(T)>>;

}  // namespace nu

#include "nu/impl/lazy.ipp"

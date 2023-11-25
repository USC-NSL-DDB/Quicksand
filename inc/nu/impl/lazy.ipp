namespace nu {

template <typename T>
inline T &Lazy<T>::get() & {
  evaluate();
  return *t_;
}

template <typename T>
inline T Lazy<T>::get() && {
  evaluate();
  return std::move(*t_);
}

template <typename T>
inline bool Lazy<T>::is_evaluated() const {
  return t_.has_value();
}

template <typename T>
inline Lazy<T>::Lazy(std::move_only_function<T()> &&fn) : fn_(std::move(fn)) {}

template <typename T>
inline Lazy<T>::Lazy(T &&t) : t_(std::move(t)) {}

template <typename T>
inline void Lazy<T>::evaluate() {
  if (!t_) {
    t_ = fn_();
  }
}

template <typename T>
inline Lazy<T> make_lazy(T t)
  requires(!std::invocable<T>)
{
  return Lazy<T>(std::move(t));
}

template <typename F>
inline auto make_lazy(F &&f) -> Lazy<std::result_of_t<F()>> {
  return Lazy<std::result_of_t<F()>>(std::forward<F>(f));
}

template <typename T, typename F>
inline auto transform_lazy(Lazy<T> &&u, F &&f) -> Lazy<std::result_of_t<F(T)>> {
  using RetT = std::result_of_t<F(T)>;
  return Lazy<RetT>([u = std::move(u), f = std::forward<F>(f)]() mutable {
    return f(std::move(u.get()));
  });
}

}  // namespace nu

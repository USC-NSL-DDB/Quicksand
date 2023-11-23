namespace nu {

template <typename T>
T &Lazy<T>::get() {
  if (!t_) {
    t_ = fn_();
  }
  return *t_;
}

template <typename T>
bool Lazy<T>::is_evaluated() const {
  return t_.has_value();
}

template <typename T>
Lazy<T>::Lazy(std::move_only_function<T()> &&fn) : fn_(std::move(fn)) {}

template <typename T>
Lazy<T>::Lazy(T &&t) : t_(std::move(t)) {}

template <typename T, typename F>
Lazy<T> make_lazy(F &&f)
  requires(!std::is_same_v<T, F>)
{
  return Lazy<T>(std::forward<F>(f));
}

template <typename T>
Lazy<T> make_lazy(T t) {
  return Lazy<T>(std::move(t));
}

template <typename T, typename F>
auto transform_lazy(Lazy<T> &&u, F &&f) -> Lazy<std::result_of_t<F(T)>> {
  using RetT = std::result_of_t<F(T)>;
  return Lazy<RetT>([u = std::move(u), f = std::forward<F>(f)]() mutable {
    return f(std::move(u.get()));
  });
}

}  // namespace nu

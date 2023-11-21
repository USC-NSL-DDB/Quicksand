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
Lazy<T>::Lazy(std::function<T()> &&fn) : fn_(std::move(fn)) {}

template <typename T>
Lazy<T> make_lazy(std::function<T()> fn) {
  return Lazy<T>(std::move(fn));
}

}  // namespace nu

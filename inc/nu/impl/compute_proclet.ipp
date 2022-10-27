namespace nu {
template <typename F, typename... As>
ComputeProclet<F, As...> compute(F&& fn, As&&... states) {
  return ComputeProclet(std::forward<F>(fn), std::forward<As>(states)...);
}

template <typename F, typename... As>
ComputeProclet<F, As...>::ComputeProclet(F&& fn, As&&... states)
    : inner_(nu::make_proclet<ComputeProclet<F, As...>::Executor>(
          std::forward<F>(fn), std::forward<As>(states)...)) {}

template <typename F, typename... As>
ComputeProclet<F, As...>::RetT ComputeProclet<F, As...>::get() {
  return inner_.run(&ComputeProclet<F, As...>::Executor::get);
}

template <typename F, typename... As>
ComputeProclet<F, As...>::Executor::Executor(F&& fn, As&&... states)
    : f_(nu::async([&]() { return fn(states...); })) {}

template <typename F, typename... As>
ComputeProclet<F, As...>::Executor::Executor(Executor&& o) {
  get();
  f_ = std::move(o.f_);
}

template <typename F, typename... As>
ComputeProclet<F, As...>::Executor&
ComputeProclet<F, As...>::Executor::operator=(Executor&& o) {
  get();
  f_ = std::move(o.f_);
}

template <typename F, typename... As>
ComputeProclet<F, As...>::Executor::~Executor() {
  get();
}

template <typename F, typename... As>
ComputeProclet<F, As...>::RetT ComputeProclet<F, As...>::Executor::get() {
  return f_.get();
}

template <typename F, typename... As>
template <class Archive>
void ComputeProclet<F, As...>::Executor::save(Archive& ar) const {
  ar(f_);
}

template <typename F, typename... As>
template <class Archive>
void ComputeProclet<F, As...>::Executor::load(Archive& ar) {
  ar(f_);
}

}  // namespace nu

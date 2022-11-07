namespace nu {

template <typename RetT, typename... A0s, typename... A1s>
ComputeProclet<RetT> make_compute_proclet(RetT (*fn)(A0s...), A1s &&... args) {
  using Fn = decltype(fn);

  ComputeProclet<RetT> cp;
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto wrapped_fn = +[](ErasedType &_, uintptr_t fn_addr, A0s... args) {
    auto *fn = reinterpret_cast<Fn>(fn_addr);
    if constexpr (std::is_same_v<RetT, void>) {
      fn(std::move(args)...);
    } else {
      return fn(std::move(args)...);
    }
  };
  cp.future_ = cp.proclet_.run_async(wrapped_fn, fn_addr, args...);

  return cp;
}

template <typename Rng, typename... A0s, typename... A1s>
ComputeProclet<void> compute_range(void (*fn)(iter_val_t<range_iter_t<Rng>> &,
                                              A0s...),
                                   Rng &&r, A1s &&... args) {
  using Fn = decltype(fn);

  ComputeProclet<void> cp;
  auto fn_addr = reinterpret_cast<uintptr_t>(fn);
  auto wrapped_fn = +[](ErasedType &_, uintptr_t fn_addr,
                        std::decay_t<Rng> range, A0s... args) {
    auto *fn = reinterpret_cast<Fn>(fn_addr);
    for (const auto &val : range) {
      fn(val, args...);
    }
  };
  cp.future_ =
      cp.proclet_.run_async(wrapped_fn, fn_addr, r, std::move(args)...);

  return cp;
}

template <typename RetT>
ComputeProclet<RetT>::ComputeProclet()
    : proclet_(nu::make_proclet<ErasedType>()) {}

template <typename RetT>
ComputeProclet<RetT>::RetTRef ComputeProclet<RetT>::get() {
  if constexpr (std::is_same_v<RetT, void>) {
    future_.get();
  } else {
    return future_.get();
  }
}

}  // namespace nu

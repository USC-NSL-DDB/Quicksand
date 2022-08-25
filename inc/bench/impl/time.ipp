namespace bench {
template <typename F, typename... Args>
inline uint64_t time(F fn, Args &&... args) {
  auto t0 = microtime();
  fn(std::forward(args)...);
  auto t1 = microtime();
  return t1 - t0;
}
}  // namespace bench

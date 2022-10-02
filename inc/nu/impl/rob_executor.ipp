namespace nu {

template <typename Arg, typename Ret>
RobExecutor<Arg, Ret>::RobExecutor(std::function<Ret(const Arg &)> fn,
                                   uint32_t rob_size)
    : fn_(std::move(fn)),
      rob_(rob_size),
      th_([&] { executor_fn(); }),
      sleeping_(false),
      done_(false) {}

template <typename Arg, typename Ret>
RobExecutor<Arg, Ret>::~RobExecutor() {
  done_ = true;
  barrier();
  for (auto &entry : rob_) {
    entry.spin.lock();
    entry.spin.unlock();
    entry.cond_var.signal();
  }
  th_.join();
}

template <typename Arg, typename Ret>
std::optional<Ret> RobExecutor<Arg, Ret>::submit_and_get(uint32_t seq,
                                                         Arg &&arg) {
  auto &entry = rob_[seq % rob_.size()];
  entry.spin.lock();
  while (entry.arg) {
    entry.cond_var.wait(&entry.spin);
    barrier();
  }
  auto ret_ptr = std::move(entry.ret);
  auto arg_ptr = std::make_unique<Arg>(std::move(arg));
  entry.arg = std::move(arg_ptr);
  entry.spin.unlock();
  entry.cond_var.signal();

  return ret_ptr ? std::move(*ret_ptr) : std::nullopt;
}

template <typename Arg, typename Ret>
std::vector<Ret> RobExecutor<Arg, Ret>::wait_all() {
  std::vector<Ret> rets;

  while (!rt::access_once(sleeping_)) {
    rt::Yield();
  }

  for (auto &entry : rob_) {
    if (entry.ret) {
      rets.emplace_back(std::move(*entry.ret));
      entry.ret.reset();
    }
  }
  return rets;
}

template <typename Arg, typename Ret>
void RobExecutor<Arg, Ret>::executor_fn() {
  while (true) {
    for (auto &entry : rob_) {
      entry.spin.lock();
      while (!entry.arg) {
        if (unlikely(rt::access_once(done_))) {
          return;
        }
        sleeping_ = true;
        entry.cond_var.wait(&entry.spin);
        sleeping_ = false;
        barrier();
      }
      auto arg_ptr = std::move(entry.arg);
      entry.spin.unlock();
      entry.cond_var.signal();

      auto ret_ptr = std::make_unique<Ret>(fn_(*arg_ptr));
      entry.ret = std::move(ret_ptr);
    }
  }
}

}

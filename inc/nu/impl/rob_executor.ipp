namespace nu {

template <typename Arg, typename Ret>
RobExecutor<Arg, Ret>::RobExecutor(std::function<Ret(const Arg &)> fn,
                                   uint32_t rob_size)
    : fn_(std::move(fn)),
      rob_(rob_size),
      th_([&] { executor_fn(); }),
      done_(false) {}

template <typename Arg, typename Ret>
RobExecutor<Arg, Ret>::~RobExecutor() {
  done_ = true;
  barrier();
  for (auto &entry : rob_) {
    entry.mutex.lock();
    entry.mutex.unlock();
    entry.cond_var.signal();
  }
  th_.join();
}

template <typename Arg, typename Ret>
std::optional<Ret> RobExecutor<Arg, Ret>::submit_and_get(uint32_t seq,
                                                         Arg &&arg) {
  auto &entry = rob_[seq % rob_.size()];
  entry.mutex.lock();
  while (entry.arg) {
    entry.cond_var.wait(&entry.mutex);
    barrier();
  }
  auto ret_ptr = std::move(entry.ret);
  auto arg_ptr = std::make_unique<Arg>(std::move(arg));
  entry.arg = std::move(arg_ptr);
  entry.mutex.unlock();
  entry.cond_var.signal();

  return ret_ptr ? std::move(*ret_ptr) : std::nullopt;
}

template <typename Arg, typename Ret>
std::vector<Ret> RobExecutor<Arg, Ret>::wait_all(uint32_t start_seq) {
  std::vector<Ret> rets;

  for (uint32_t i = 0; i < rob_.size(); i++) {
    auto idx = (start_seq + i) % rob_.size();
    auto &entry = rob_[idx];
    entry.mutex.lock();
    while (entry.arg) {
      entry.cond_var.wait(&entry.mutex);
      barrier();
    }
    entry.mutex.unlock();
    if (!entry.ret) {
      break;
    }
    rets.emplace_back(std::move(*entry.ret));
    entry.ret.reset();
  }
  return rets;
}

template <typename Arg, typename Ret>
void RobExecutor<Arg, Ret>::executor_fn() {
  while (true) {
    for (auto &entry : rob_) {
      entry.mutex.lock();
      while (!entry.arg) {
        if (unlikely(rt::access_once(done_))) {
          entry.mutex.unlock();
          return;
        }
        entry.cond_var.wait(&entry.mutex);
        barrier();
      }
      auto arg_ptr = std::move(entry.arg);
      auto ret_ptr = std::make_unique<Ret>(fn_(*arg_ptr));
      entry.ret = std::move(ret_ptr);
      entry.mutex.unlock();
      entry.cond_var.signal();
    }
  }
}

}

#include <nu/utils/scoped_lock.hpp>
#include <utility>

namespace nu {

template <class Impl, class Synchronized>
template <typename RetT, typename F>
RetT GeneralContainerBase<Impl, Synchronized>::synchronized(F &&f) {
  if constexpr (Synchronized::value) {
    ScopedLock<Mutex> guard(&mutex_);
    return f();
  } else {
    return f();
  }
}

template <class Impl, class Synchronized>
void GeneralContainerBase<Impl, Synchronized>::emplace_batch(
    std::vector<std::pair<Key, Val>> reqs) {
  synchronized<void>([&]() {
    for (auto &req : reqs) {
      impl_.emplace(std::move(req.first), std::move(req.second));
    }
  });
}

template <class Impl, class Synchronized>
void GeneralContainerBase<Impl, Synchronized>::emplace_back_batch(
    std::vector<Val> reqs) {
  synchronized<void>([&]() {
    for (auto &req : reqs) {
      if constexpr (EmplaceBackAble<Impl>) {
        impl_.emplace_back(std::move(req));
      }
    }
  });
}

}  // namespace nu

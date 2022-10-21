#include <nu/utils/scoped_lock.hpp>
#include <utility>

namespace nu {

template <class Impl, class Synchronized>
template <typename RetT, typename F>
inline RetT GeneralContainerBase<Impl, Synchronized>::synchronized(
    F &&f) const {
  if constexpr (Synchronized::value) {
    ScopedLock<Mutex> guard(const_cast<Mutex *>(&mutex_));
    return f();
  } else {
    return f();
  }
}

template <class Impl, class Synchronized>
inline void GeneralContainerBase<Impl, Synchronized>::emplace_batch(
    std::vector<std::pair<Key, Val>> reqs) {
  synchronized<void>([&]() {
    for (auto &req : reqs) {
      impl_.emplace(std::move(req.first), std::move(req.second));
    }
  });
}

}  // namespace nu

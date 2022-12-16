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
inline std::size_t GeneralContainerBase<Impl, Synchronized>::insert_batch(
    std::vector<DataEntry> reqs) requires InsertAble<Impl> {
  return synchronized<std::size_t>([&]() {
    for (auto &req : reqs) {
      if constexpr (HasVal<Impl>) {
        impl_.insert(std::move(req.first), std::move(req.second));
      } else {
        impl_.insert(std::move(req));
      }
    }
    return impl_.size();
  });
}

}  // namespace nu

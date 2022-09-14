#include <nu/utils/scoped_lock.hpp>
#include <utility>

namespace nu {

template <typename Key, typename Val>
template <class Archive>
void ContainerReq<Key, Val>::serialize(Archive &ar) {
  ar(type, k, v);
}

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
void GeneralContainerBase<Impl, Synchronized>::handle_batch(
    std::vector<ContainerReq<Key, Val>> reqs) {
  synchronized<void>([&]() {
    for (auto &req : reqs) {
      if (req.type == Emplace) {
        impl_.emplace(std::move(req.k), std::move(req.v));
      } else if (req.type == EmplaceBack) {
        if constexpr (EmplaceBackAble<Impl>) {
          impl_.emplace_back(std::move(req.v));
        }
      } else {
        BUG();
      }
    }
  });
}

template <class Impl, class Synchronized>
void GeneralContainerBase<Impl, Synchronized>::on_key_range_updated(
    std::optional<Key> l_key, std::optional<Key> r_key) {
  constexpr bool has_key_range_update_hook = requires(Impl t) {
    {
      t.on_key_range_updated(std::declval<std::optional<typename Impl::Key>>(),
                             std::declval<std::optional<typename Impl::Key>>())
    }
    ->std::same_as<void>;
  };

  if constexpr (has_key_range_update_hook) {
    synchronized<void>([&]() {
      impl_.on_key_range_updated(std::move(l_key), std::move(r_key));
    });
  }
}

}  // namespace nu

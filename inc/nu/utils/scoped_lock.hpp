#pragma once

namespace nu {

template <typename L>
class ScopedLock {
 public:
  ScopedLock(const ScopedLock &) = delete;
  ScopedLock &operator=(const ScopedLock &) = delete;
  ScopedLock(ScopedLock &&o) noexcept : l_(o.l_) { o.l_ = nullptr; }
  ScopedLock &operator=(ScopedLock &&o) noexcept {
    l_ = o.l_;
    o.l_ = nullptr;
    return *this;
  }
  ScopedLock(L *l) : l_(l) { l->lock(); }
  ~ScopedLock() {
    if (l_) {
      l_->unlock();
    }
  }

 private:
  L *l_;
};

}  // namespace nu

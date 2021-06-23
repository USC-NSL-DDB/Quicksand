#pragma once

#include <memory>

#include "rem_ptr.hpp"

namespace nu {

template <typename T> class RemSharedPtr : public RemPtr<T> {
public:
  RemSharedPtr() noexcept;
  RemSharedPtr(std::shared_ptr<T> &&shared_ptr) noexcept;
  ~RemSharedPtr() noexcept;
  RemSharedPtr(const RemSharedPtr &) noexcept;
  RemSharedPtr &operator=(const RemSharedPtr &) noexcept;
  RemSharedPtr(RemSharedPtr &&) noexcept;
  RemSharedPtr &operator=(RemSharedPtr &&) noexcept;
  void reset();
  Future<void> reset_async();
  void reset_bg();

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void save(Archive &ar);
  template <class Archive> void load(Archive &ar);

private:
  RemSharedPtr(std::shared_ptr<T> *shared_ptr);

  std::shared_ptr<T> *shared_ptr_ = nullptr;

  template <typename U, typename... Args>
  friend RemSharedPtr<U> make_rem_shared(Args &&... args);
};

template <typename T, typename... Args>
RemSharedPtr<T> make_rem_shared(Args &&... args);

} // namespace nu

#include "impl/rem_shared_ptr.ipp"

#pragma once

#include <memory>

#include "rem_ptr.hpp"

namespace nu {

template <typename T> class RemUniquePtr : public RemPtr<T> {
public:
  RemUniquePtr();
  RemUniquePtr(std::unique_ptr<T> &&unique_ptr);
  ~RemUniquePtr();
  RemUniquePtr(const RemUniquePtr &) = delete;
  RemUniquePtr &operator=(const RemUniquePtr &) = delete;
  RemUniquePtr(RemUniquePtr &&);
  RemUniquePtr &operator=(RemUniquePtr &&);
  void release();
  void reset();
  Future<void> reset_async();
  void reset_bg();

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void save(Archive &ar);

private:
  template <typename U, typename... Args>
  friend RemUniquePtr<U> make_rem_unique(Args &&... args);

  RemUniquePtr(T *raw_ptr);
};

template <typename T, typename... Args>
RemUniquePtr<T> make_rem_unique(Args &&... args);

} // namespace nu

#include "impl/rem_unique_ptr.ipp"

#pragma once

#include <memory>

#include "rem_ptr.hpp"

namespace nu {

template <typename T> class RemSharedPtr : public RemPtr<T> {
public:
  RemSharedPtr();
  RemSharedPtr(std::shared_ptr<T> &&shared_ptr);
  ~RemSharedPtr();
  RemSharedPtr(const RemSharedPtr &);
  RemSharedPtr &operator=(const RemSharedPtr &);
  RemSharedPtr(RemSharedPtr &&);
  RemSharedPtr &operator=(RemSharedPtr &&);
  void reset();
  Future<void> reset_async();
  void reset_bg();

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void save(Archive &ar);
  template <class Archive> void load(Archive &ar);

private:
  std::shared_ptr<T> *shared_ptr_ = nullptr;
};

template <typename T, typename... Args>
RemSharedPtr<T> make_rem_shared(Args &&... args);

} // namespace nu

#include "impl/rem_shared_ptr.ipp"

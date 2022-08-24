#pragma once

#include "nu/sharded_ds.hpp"
#include "nu/type_traits.hpp"

namespace nu {

template <typename T>
class SealedDS {
  static_assert(is_base_of_template_v<T, ShardedDataStructure>);

 public:
  class ConstIterator {};
  class ConstReverseIterator();

  ConstIterator cbegin();
  ConstIterator cend();
  T &&unseal();
  ConstReverseIterator crbegin();
  ConstReverseIterator crend();

 private:
  template <typename U>
  friend SealedDS<U> make_sealed_ds(U &&u);

  SealedDS(T &&t);
};

template <typename T>
SealedDS<T> make_sealed_ds(T &&t) {
  return SealedDS<T>(std::move(t));
}
}  // namespace nu

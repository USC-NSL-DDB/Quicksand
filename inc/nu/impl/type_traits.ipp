#include "nu/proclet.hpp"
#include "nu/rem_shared_ptr.hpp"
#include "nu/rem_unique_ptr.hpp"

namespace nu {

class DistributedMemPool;

template <typename T> auto move_if_safe(T &&t) {
  // TODO: add DistributedHashTable
  if constexpr (is_specialization_of_v<std::decay_t<T>, Proclet> ||
                is_specialization_of_v<std::decay_t<T>, RemUniquePtr> ||
                is_specialization_of_v<std::decay_t<T>, RemSharedPtr> ||
                std::is_same_v<std::decay_t<T>, DistributedMemPool>) {
    return std::move(t);
  } else {
    return t;
  }
}

} // namespace nu

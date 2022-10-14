namespace nu {

class DistributedMemPool;
template <typename T>
class Proclet;
template <typename T>
class RemUniquePtr;
template <typename T>
class RemSharedPtr;

template <class T>
T& to_lvalue_ref(T&& t) {
  return t;
}

template <typename T>
auto &&move_if_safe(T &&t) {
  if constexpr (std::is_rvalue_reference_v<T &&> &&
                // TODO: add more safe-to-move types.
                (is_specialization_of_v<std::decay_t<T>, Proclet> ||
                 is_specialization_of_v<std::decay_t<T>, RemUniquePtr> ||
                 is_specialization_of_v<std::decay_t<T>, RemSharedPtr> ||
                 std::is_same_v<std::decay_t<T>, DistributedMemPool>)) {
    // Safe to move.
    return std::move(t);
  } else {
    // Otherwise copy.
    return to_lvalue_ref(t);
  }
}

}  // namespace nu

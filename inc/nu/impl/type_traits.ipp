namespace nu {

class DistributedMemPool;
template <typename T>
class Proclet;
template <typename T>
class RemUniquePtr;
template <typename T>
class RemSharedPtr;

template <class T>
inline consteval bool is_move_safe() {
  return std::is_rvalue_reference_v<T> &&
         // TODO: add more safe-to-move types.
         (is_specialization_of_v<std::decay_t<T>, Proclet> ||
          is_specialization_of_v<std::decay_t<T>, RemUniquePtr> ||
          is_specialization_of_v<std::decay_t<T>, RemSharedPtr> ||
          std::is_same_v<std::decay_t<T>, DistributedMemPool>);
}

template <typename T>
inline auto &&move_if_safe(T &&t) requires(is_move_safe<T &&>()) {
  return std::move(t);
}

template <typename T>
inline auto move_if_safe(T &&t) requires DeepCopyAble<T> {
  return t.deep_copy();
}

template <typename T>
inline T &to_lvalue_ref(T &&t) {
  return t;
}

template <typename T>
inline auto &move_if_safe(T &&t) {
  return to_lvalue_ref(t);
}

}  // namespace nu

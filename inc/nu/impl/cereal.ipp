namespace cereal {

template <class T>
consteval bool is_memcpy_safe() {
  if constexpr (std::is_trivially_copy_assignable_v<T>) {
    return true;
  } else if constexpr (nu::is_specialization_of_v<T, std::pair>) {
    return is_memcpy_safe<typename T::first_type>() &&
           is_memcpy_safe<typename T::second_type>();
  } else if constexpr (nu::is_specialization_of_v<T, std::tuple>) {
    return std::apply(
        []<typename... Args>(Args... _) {
          return (is_memcpy_safe<Args>() && ...);
        },
        T{});
  }
  return false;
}

template <class Archive, typename T>
inline void save(Archive &ar, T const &t) requires(
    std::is_trivially_copy_assignable_v<T> &&
    !std::is_fundamental_v<T> &&
    !std::is_pointer_v<T> &&
    !HasBuiltinSerialize<Archive, T> &&
    !HasBuiltinSave<Archive, T> &&
    !HasBuiltinLoad<Archive, T> &&
    !nu::is_specialization_of_v<T, std::optional> &&
    !nu::is_specialization_of_v<T, cereal::BinaryData>) {
  ar(cereal::binary_data(&t, sizeof(T)));
}

template <class Archive, typename T>
inline void load(Archive &ar, T &t) requires(
    std::is_trivially_copy_assignable_v<T> &&
    !std::is_fundamental_v<T> &&
    !std::is_pointer_v<T> &&
    !HasBuiltinSerialize<Archive, T> &&
    !HasBuiltinSave<Archive, T> &&
    !HasBuiltinLoad<Archive, T> &&
    !nu::is_specialization_of_v<T, std::optional> &&
    !nu::is_specialization_of_v<T, cereal::BinaryData>) {
  ar(cereal::binary_data(&t, sizeof(T)));
}

template <class Archive, typename P, typename A>
inline void save(Archive &ar, std::vector<P, A> const &v) requires(
    is_memcpy_safe<P>()) {
  ar(v.size());
  ar(cereal::binary_data(v.data(), v.size() * sizeof(P)));
}

template <class Archive, typename P, typename A>
inline void save_move(Archive &ar, std::vector<P, A> &&v) requires(
    is_memcpy_safe<P>()) {
  ar(v.size());
  ar(cereal::binary_data(v.data(), v.size() * sizeof(P)));
}

template <class Archive, typename P, typename A>
inline void load(Archive &ar, std::vector<P, A> &v) requires(
    is_memcpy_safe<P>()) {
  decltype(v.size()) size;
  ar(size);
  v.resize(size);
  ar(cereal::binary_data(v.data(), size * sizeof(P)));
}

}

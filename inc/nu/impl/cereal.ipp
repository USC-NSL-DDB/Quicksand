namespace cereal {

template <class Archive, typename T>
void save(Archive &ar, T const &t) requires(
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
void load(Archive &ar, T &t) requires(
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
void save(Archive &ar, std::vector<P, A> const &v) requires(
    is_memcpy_safe<P>()) {
  ar(v.size());
  ar(cereal::binary_data(v.data(), v.size() * sizeof(P)));
}

template <class Archive, typename P, typename A>
void save_move(Archive &ar, std::vector<P, A> &&v) requires(
    is_memcpy_safe<P>()) {
  ar(v.size());
  ar(cereal::binary_data(v.data(), v.size() * sizeof(P)));
}

template <class Archive, typename P, typename A>
void load(Archive &ar, std::vector<P, A> &v) requires(
    is_memcpy_safe<P>()) {
  decltype(v.size()) size;
  ar(size);
  v.resize(size);
  ar(cereal::binary_data(v.data(), size * sizeof(P)));
}

}

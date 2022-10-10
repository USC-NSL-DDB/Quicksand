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

template <class Archive, typename V, typename I, typename A>
void save(Archive &ar, std::vector<std::pair<V, I>, A> const &v) requires(
    std::is_trivially_copy_assignable_v<V> &&
    std::is_trivially_copy_assignable_v<I>) {
  ar(v.size());
  ar(cereal::binary_data(v.data(), v.size() * sizeof(std::pair<V, I>)));
}

template <class Archive, typename V, typename I, typename A>
void save_move(Archive &ar, std::vector<std::pair<V, I>, A> &&v) requires(
    std::is_trivially_copy_assignable_v<V> &&
    std::is_trivially_copy_assignable_v<I>) {
  ar(v.size());
  ar(cereal::binary_data(v.data(), v.size() * sizeof(std::pair<V, I>)));
}

template <class Archive, typename V, typename I, typename A>
void load(Archive &ar, std::vector<std::pair<V, I>, A> &v) requires(
    std::is_trivially_copy_assignable_v<V> &&
    std::is_trivially_copy_assignable_v<I>) {
  decltype(v.size()) size;
  ar(size);
  v.resize(size);
  ar(cereal::binary_data(v.data(), size * sizeof(std::pair<V, I>)));
}

}

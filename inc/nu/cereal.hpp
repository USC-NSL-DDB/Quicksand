#pragma once

#include <cereal/types/deque.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/queue.hpp>
#include <cereal/types/set.hpp>
#include <cereal/types/stack.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/tuple.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/types/unordered_set.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/archives/binary.hpp>
#include <concepts>
#include <type_traits>

#include "nu/type_traits.hpp"

namespace cereal {

template <class Archive, class T>
concept HasBuiltinSerialize = requires(Archive ar, T t) {
    { t.serialize(ar) };
};

template <class Archive, class T>
concept HasBuiltinSave = requires(Archive ar, const T &t) {
    { t.save(ar) };
};

template <class Archive, class T>
concept HasBuiltinLoad = requires(Archive ar, T t) {
    { t.load(ar) };
};

template <class T>
consteval bool is_memcpy_safe();

template <class Archive, typename T>
void save(Archive &ar, T const &t) requires(
    std::is_trivially_copy_assignable_v<T> &&
    !std::is_fundamental_v<T> &&
    !std::is_pointer_v<T> &&
    !HasBuiltinSerialize<Archive, T> &&
    !HasBuiltinSave<Archive, T> &&
    !HasBuiltinLoad<Archive, T> &&
    !nu::is_specialization_of_v<T, std::optional> &&
    !nu::is_specialization_of_v<T, cereal::BinaryData>);

template <class Archive, typename T>
void load(Archive &ar, T &t) requires(
    std::is_trivially_copy_assignable_v<T> &&
    !std::is_fundamental_v<T> &&
    !std::is_pointer_v<T> &&
    !HasBuiltinSerialize<Archive, T> &&
    !HasBuiltinSave<Archive, T> &&
    !HasBuiltinLoad<Archive, T> &&
    !nu::is_specialization_of_v<T, std::optional> &&
    !nu::is_specialization_of_v<T, cereal::BinaryData>);

template <class Archive, typename P, typename A>
void save(Archive &ar, std::vector<P, A> const &v) requires(
    is_memcpy_safe<P>());

template <class Archive, typename P, typename A>
void save_move(Archive &ar, std::vector<P, A> &&v) requires(
    is_memcpy_safe<P>());

template <class Archive, typename P, typename A>
void load(Archive &ar, std::vector<P, A> &v) requires(
    is_memcpy_safe<P>());

}  // namespace cereal

#include "nu/impl/cereal.ipp"

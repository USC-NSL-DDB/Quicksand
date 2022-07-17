#pragma once

#include <type_traits>

namespace nu {

template <typename T, template <typename...> class Template>
struct is_specialization_of : std::false_type {};

template <template <typename...> class Template, typename... Args>
struct is_specialization_of<Template<Args...>, Template> : std::true_type {};

template <class T, template <class...> class Template>
constexpr bool is_specialization_of_v =
    is_specialization_of<T, Template>::value;

template <typename T>
auto move_if_safe(T &&t);

template <template <typename...> class C, typename... Ts>
std::true_type is_base_of_template_impl(const C<Ts...> *);

template <template <typename...> class C>
std::false_type is_base_of_template_impl(...);

template <typename T, template <typename...> class C>
using is_base_of_template =
    decltype(is_base_of_template_impl<C>(std::declval<T *>()));

template <typename T, template <typename...> class C>
inline constexpr bool is_base_of_template_v = is_base_of_template<T, C>::value;

}  // namespace nu

#include "nu/impl/type_traits.ipp"

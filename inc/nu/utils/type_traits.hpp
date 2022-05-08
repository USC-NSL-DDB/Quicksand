#pragma once

#include <type_traits>

template <typename T, template <typename...> class Template>
struct is_specialization_of : std::false_type {};

template <template <typename...> class Template, typename... Args>
struct is_specialization_of<Template<Args...>, Template> : std::true_type {};

template <class T, template <class...> class Template>
constexpr bool is_specialization_of_v =
    is_specialization_of<T, Template>::value;

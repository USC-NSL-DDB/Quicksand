#pragma once

template <typename... T> void assert_no_pointer_or_lval_ref() {
  static_assert((!std::is_lvalue_reference<T>::value && ... && true));
  static_assert((!std::is_pointer<T>::value && ... && true));
}

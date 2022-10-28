#pragma once

#include <type_traits>

#include "nu/commons.hpp"
#include "nu/proclet.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename RetT>
class ComputeProclet {
 public:
  using RetTRef = decltype([] {
    if constexpr (!std::is_same_v<RetT, void>) {
      return RetT();
    }
  }());

  ComputeProclet(ComputeProclet&&) = default;
  ComputeProclet& operator=(ComputeProclet&&) = default;
  ComputeProclet(const ComputeProclet&) = delete;
  ComputeProclet& operator=(const ComputeProclet&) = delete;

  RetTRef get();

 private:
  Proclet<ErasedType> proclet_;
  Future<RetT> future_;

  template <typename R, typename... A0s, typename... A1s>
  friend ComputeProclet<R> make_compute_proclet(R (*fn)(A0s...), A1s&&...);

  ComputeProclet();
};

template <typename RetT, typename... A0s, typename... A1s>
ComputeProclet<RetT> make_compute_proclet(RetT (*fn)(A0s...), A1s&&... args);

}  // namespace nu

#include "nu/impl/compute_proclet.ipp"

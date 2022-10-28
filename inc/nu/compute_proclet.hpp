#pragma once

#include "nu/proclet.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/utils/future.hpp"

namespace nu {
template <typename F, typename... As>
class ComputeProclet {
 private:
  using RetT = std::invoke_result_t<std::decay_t<F>, As&...>;

 public:
  ComputeProclet(F&& fn, As&&... states);
  ComputeProclet(const ComputeProclet&) = default;
  ComputeProclet& operator=(const ComputeProclet&) = default;
  ComputeProclet(ComputeProclet&&) = default;
  ComputeProclet& operator=(ComputeProclet&&) = default;

  RetT get();

 private:
  class Executor {
   public:
    Executor(F&& fn, As&&... states);
    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;
    Executor(Executor&&);
    Executor& operator=(Executor&&);
    ~Executor();

    RetT get();
    template <class Archive>
    void save(Archive& ar) const;
    template <class Archive>
    void load(Archive& ar);

   private:
    Future<RetT> f_;
  };

  Proclet<Executor> inner_;
};

template <typename F, typename R, typename... As>
class RangedComputeProclet {
 private:
 public:
  RangedComputeProclet(F&& fn, R&& range, As&&... states);
  RangedComputeProclet(const RangedComputeProclet&) = default;
  RangedComputeProclet& operator=(const RangedComputeProclet&) = default;
  RangedComputeProclet(RangedComputeProclet&&) = default;
  RangedComputeProclet& operator=(RangedComputeProclet&&) = default;

  void get();

 private:
  class Executor {
   public:
    Executor(F&& fn, R&& range, As&&... states);
    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;
    Executor(Executor&&);
    Executor& operator=(Executor&&);
    ~Executor();

    void get();
    template <class Archive>
    void save(Archive& ar) const;
    template <class Archive>
    void load(Archive& ar);

   private:
    Future<void> f_;
  };

  Proclet<Executor> inner_;
};

template <typename F, typename... As>
ComputeProclet<F, As...> compute(F&& fn, As&&... states);

template <typename F, typename R, typename... As>
RangedComputeProclet<F, R, As...> compute_range(F&& fn, R&& range,
                                                As&&... states);
}  // namespace nu

#include "nu/impl/compute_proclet.ipp"

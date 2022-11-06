#pragma once

#include "nu/commons.hpp"

namespace nu {

class Counter {
 public:
  Counter();
  void inc();
  void dec();
  void inc_unsafe();
  void dec_unsafe();
  int64_t get() const;
  void reset();

 private:
  struct alignas(kCacheLineBytes) {
    int64_t c;
  } cnts_[kNumCores];
};

}  // namespace nu

#include "nu/impl/counter.ipp"

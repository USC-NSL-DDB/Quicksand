#pragma once

#include <cstdint>

namespace nu {

class SplitMix64 {
 public:
  SplitMix64();
  SplitMix64(const SplitMix64 &) = default;
  SplitMix64(SplitMix64 &&) = default;
  SplitMix64 &operator=(const SplitMix64 &) = default;
  SplitMix64 &operator=(SplitMix64 &&) = default;
  SplitMix64(uint64_t seed);
  uint64_t next();

 private:
  uint64_t x_;
};

}  // namespace nu

#include "nu/impl/splitmix64.ipp"

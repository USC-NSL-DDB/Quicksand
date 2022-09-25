#pragma once

namespace bench {

/**
 * Measures how long a function takes to execute in microseconds.
 */
template <typename F, typename... Args>
uint64_t time(F fn, Args &&... args);

}  // namespace bench

#include "bench/impl/time.ipp"

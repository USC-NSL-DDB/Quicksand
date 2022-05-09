#pragma once

#include <cstdint>
#include <list>

extern "C" {
#include <base/time.h>
#include <runtime/timer.h>
}
#include <sync.h>

namespace nu {

class Time {
public:
  constexpr static uint64_t kMilliseconds = 1000;
  constexpr static uint64_t kSeconds = 1000000;

  Time();
  static uint64_t rdtsc();
  static uint64_t microtime();
  static void delay(uint64_t us);
  static void sleep_until(uint64_t deadline_us);
  static void sleep(uint64_t duration_us);

private:
  int64_t offset_tsc_;
  std::list<timer_entry *> entries_;
  rt::Spin spin_;
  friend class Migrator;

  static void timer_callback(unsigned long arg_addr);
  uint64_t proclet_env_microtime();
  uint64_t proclet_env_rdtsc();
  void proclet_env_sleep(uint64_t duration_us);
  void proclet_env_sleep_until(uint64_t deadline_us);
  uint64_t to_logical_tsc(uint64_t physical_tsc);
  uint64_t to_logical_us(uint64_t physical_us);
  uint64_t to_physical_tsc(uint64_t logical_tsc);
  uint64_t to_physical_us(uint64_t logical_us);
};

struct TimerCallbackArg {
  thread_t *th;
  ProcletHeader *proclet_header;
  uint64_t logical_deadline_us;
  std::list<timer_entry *>::iterator iter;
};

} // namespace nu

#include "nu/impl/time.ipp"

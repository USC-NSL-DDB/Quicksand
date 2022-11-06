#include <memory>

extern "C" {
#include <base/assert.h>
}

#include "nu/runtime.hpp"
#include "nu/runtime_deleter.hpp"
#include "nu/utils/time.hpp"

namespace nu {

void Time::timer_callback(unsigned long arg_addr) {
  auto *arg = reinterpret_cast<TimerCallbackArg *>(arg_addr);
  auto *proclet_header = arg->proclet_header;

  auto optional_migration_guard =
      Runtime::attach_and_disable_migration(proclet_header);
  if (unlikely(!optional_migration_guard)) {
    return;
  }
  Runtime::detach(*optional_migration_guard);

  auto &time = proclet_header->time;
  time.spin_.Lock();
  time.entries_.erase(arg->iter);
  time.spin_.Unlock();

  thread_ready(arg->th);
}

uint64_t Time::rdtsc() {
  auto *proclet_header = Runtime::get_current_proclet_header();
  if (proclet_header) {
    return proclet_header->time.proclet_env_rdtsc();
  } else {
    return ::rdtsc();
  }
}

uint64_t Time::microtime() {
  auto *proclet_header = Runtime::get_current_proclet_header();
  if (proclet_header) {
    return proclet_header->time.proclet_env_microtime();
  } else {
    return ::microtime();
  }
}

void Time::sleep_until(uint64_t deadline_us) {
  auto *proclet_header = Runtime::get_current_proclet_header();
  if (proclet_header) {
    proclet_header->time.proclet_env_sleep_until(deadline_us);
  } else {
    timer_sleep_until(deadline_us);
  };
}

void Time::sleep(uint64_t duration_us) {
  auto *proclet_header = Runtime::get_current_proclet_header();
  if (proclet_header) {
    proclet_header->time.proclet_env_sleep(duration_us);
  } else {
    timer_sleep(duration_us);
  };
}

void Time::proclet_env_sleep_until(uint64_t deadline_us) {
  auto *e = new timer_entry();
  std::unique_ptr<timer_entry> e_gc(e);
  auto physical_us = to_physical_us(deadline_us);
  auto *arg = new TimerCallbackArg();
  std::unique_ptr<TimerCallbackArg> arg_gc(arg);

  arg->th = thread_self();
  arg->proclet_header = Runtime::get_current_proclet_header();
  arg->logical_deadline_us = deadline_us;
  BUG_ON(!arg->proclet_header);
  timer_init(e, Time::timer_callback, reinterpret_cast<unsigned long>(arg));

  spin_.Lock();
  {
    RuntimeSlabGuard g;
    entries_.push_back(e);
  }
  arg->iter = --entries_.end();
  timer_start(e, physical_us);
  thread_park_and_unlock_np(reinterpret_cast<spinlock_t *>(&spin_));
}

}  // namespace nu

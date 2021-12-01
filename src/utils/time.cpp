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

  auto *heap_header = arg->heap_header;
  if (unlikely(!rt::access_once(heap_header->present))) {
    return;
  }

  auto &time = heap_header->time;
  time.spin_.Lock();
  {
    RuntimeHeapGuard g;
    time.entries_.erase(arg->iter);
  }
  time.spin_.Unlock();
  thread_ready(arg->th);
}

uint64_t Time::rdtsc() {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (heap_header) {
    return heap_header->time.obj_env_rdtsc();
  } else {
    return ::rdtsc();
  }
}

uint64_t Time::microtime() {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (heap_header) {
    return heap_header->time.obj_env_microtime();
  } else {
    return ::microtime();
  }
}

void Time::sleep_until(uint64_t deadline_us) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (heap_header) {
    heap_header->time.obj_env_sleep_until(deadline_us);
  } else {
    timer_sleep_until(deadline_us);
  };
}

void Time::sleep(uint64_t duration_us) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  if (heap_header) {
    heap_header->time.obj_env_sleep(duration_us);
  } else {
    timer_sleep(duration_us);
  };
}

void Time::obj_env_sleep_until(uint64_t deadline_us) {
  auto *e = new timer_entry();
  std::unique_ptr<timer_entry> e_gc(e);
  auto physical_us = to_physical_us(deadline_us);
  auto *arg = new TimerCallbackArg();
  std::unique_ptr<TimerCallbackArg> arg_gc(arg);

  arg->th = thread_self();
  arg->heap_header = Runtime::get_current_obj_heap_header();
  arg->logical_deadline_us = deadline_us;
  BUG_ON(!arg->heap_header);
  timer_init(e, Time::timer_callback, reinterpret_cast<unsigned long>(arg));

  spin_.Lock();
  {
    RuntimeHeapGuard g;
    entries_.push_back(e);
  }
  arg->iter = --entries_.end();
  timer_start(e, physical_us);
  thread_park_and_unlock_np(reinterpret_cast<spinlock_t *>(&spin_));
}

} // namespace nu

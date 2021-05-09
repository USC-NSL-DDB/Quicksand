#include "time.hpp"
#include "runtime.hpp"
#include "runtime_deleter.hpp"

extern "C" {
#include <base/assert.h>
}

#include <memory>

namespace nu {

void Time::timer_callback(unsigned long arg_addr) {
  auto *arg = reinterpret_cast<TimerCallbackArg *>(arg_addr);
  Time *time;

  Runtime::heap_manager->rcu_reader_lock();
  if (unlikely(!Runtime::heap_manager->contains(arg->heap_header))) {
    goto done;
  }

  time = arg->heap_header->time.get();
  time->spin_.Lock();
  time->entries_.erase(arg->iter);
  time->spin_.Unlock();
  thread_ready(arg->th);

done:
  Runtime::heap_manager->rcu_reader_unlock();
}

uint64_t Time::microtime() {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    return heap_header->time->obj_env_microtime();
  } else {
    return ::microtime();
  }
}

void Time::sleep_until(uint64_t deadline_us) {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    heap_header->time->obj_env_sleep_until(deadline_us);
  } else {
    timer_sleep_until(deadline_us);
  };
}

void Time::sleep(uint64_t duration_us) {
  auto *heap_header = Runtime::get_obj_heap_header();
  if (heap_header) {
    heap_header->time->obj_env_sleep(duration_us);
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
  arg->heap_header = Runtime::get_obj_heap_header();
  arg->logical_deadline_us = deadline_us;
  BUG_ON(!arg->heap_header);
  timer_init(e, Time::timer_callback, reinterpret_cast<unsigned long>(arg));

  spin_.Lock();
  entries_.push_back(e);
  arg->iter = --entries_.end();
  timer_start(e, physical_us);
  thread_park_and_unlock_np(reinterpret_cast<spinlock_t *>(&spin_));
}

} // namespace nu

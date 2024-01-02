#include "nu/runtime.hpp"
#include "nu/utils/thread.hpp"
#include "nu/utils/cpu_load.hpp"

namespace nu {

__attribute__((optimize("no-omit-frame-pointer"))) void
Thread::trampoline_in_proclet_env(void *args) {
  ProcletHeader *proclet_header;
  {
    Caladan::PreemptGuard g;

    proclet_header = get_runtime()->get_current_proclet_header();
    proclet_header->slab_ref_cnt.inc(g);
  }

  auto *d = reinterpret_cast<join_data *>(args);
  d->func();

  {
    Caladan::PreemptGuard g;

    CPULoad::end_monitor();
    // At this point, it's safe to access proclet_header since the object is
    // guaranteed to be alive.
    proclet_header->slab_ref_cnt.dec(g);
    proclet_header->thread_cnt.dec(g);
  }

  d->lock.lock();
  if (d->done) {
    d->cv.signal();
    d->lock.unlock();
  } else {
    d->done = true;
    d->cv.wait(&d->lock);
    d->lock.unlock();
  }
  delete d;

  {
    Caladan::PreemptGuard g;

    get_runtime()->caladan()->thread_unset_owner_proclet(Caladan::thread_self(),
                                                         true);
    // After this point, it's safe to migrate the proclet without migrating this
    // thread.
  }

  auto runtime_stack_base =
      get_runtime()->caladan()->thread_get_runtime_stack_base();
  auto old_rsp = get_runtime()->switch_stack(runtime_stack_base);
  get_runtime()->switch_to_runtime_slab();

  auto proclet_stack_addr =
      ((reinterpret_cast<uintptr_t>(old_rsp) + kStackSize - 1) &
       (~(kStackSize - 1)));
  get_runtime()->stack_manager()->put(
      reinterpret_cast<uint8_t *>(proclet_stack_addr));
  get_runtime()->caladan()->thread_exit();
}

void Thread::trampoline_in_runtime_env(void *args) {
  auto *d = reinterpret_cast<join_data *>(args);

  d->func();
  d->lock.lock();
  if (d->done) {
    d->cv.signal();
    d->lock.unlock();
  } else {
    d->done = true;
    d->cv.wait(&d->lock);
    d->lock.unlock();
  }
  std::destroy_at(&d->func);
}

void Thread::join() {
  BUG_ON(!join_data_);

  join_data_->lock.lock();
  if (join_data_->done) {
    join_data_->cv.signal();
    join_data_->lock.unlock();
    join_data_ = nullptr;
    return;
  }

  join_data_->done = true;
  join_data_->cv.wait_and_unlock(&join_data_->lock);
  join_data_ = nullptr;
}

void Thread::detach() {
  BUG_ON(!join_data_);

  join_data_->lock.lock();
  if (join_data_->done) {
    join_data_->cv.signal();
    join_data_->lock.unlock();
    join_data_ = nullptr;
    return;
  }

  join_data_->done = true;
  join_data_->lock.unlock();
  join_data_ = nullptr;
}

Thread::Thread(std::move_only_function<void()> f, bool high_priority) {
  ProcletHeader *proclet_header;
  {
    Caladan::PreemptGuard g;

    proclet_header = get_runtime()->get_current_proclet_header();
  }

  if (proclet_header) {
    create_in_proclet_env(std::move(f), proclet_header, high_priority);
  } else {
    create_in_runtime_env(std::move(f), high_priority);
  }
}

void Thread::create_in_proclet_env(std::move_only_function<void()> f,
                                   ProcletHeader *header, bool head) {
  Caladan::PreemptGuard g;

  auto *proclet_stack = get_runtime()->stack_manager()->get();
  auto proclet_stack_addr = reinterpret_cast<uint64_t>(proclet_stack);
  assert(proclet_stack_addr % kStackAlignment == 0);
  id_ = proclet_stack_addr;
  join_data_ = new join_data(std::move(f), header);
  BUG_ON(!join_data_);
  auto *th = get_runtime()->caladan()->thread_nu_create_with_args(
      proclet_stack, kStackSize, trampoline_in_proclet_env, join_data_);
  BUG_ON(!th);
  header->thread_cnt.inc(g);
  auto *caladan = get_runtime()->caladan();
  if (head) {
    caladan->thread_ready_head(th);
  } else {
    caladan->thread_ready(th);
  }
}

void Thread::create_in_runtime_env(std::move_only_function<void()> f,
                                   bool head) {
  auto *th = get_runtime()->caladan()->thread_create_with_buf(
      trampoline_in_runtime_env, reinterpret_cast<void **>(&join_data_),
      sizeof(*join_data_));
  id_ = get_runtime()->caladan()->get_thread_id(th);
  BUG_ON(!th);
  new (join_data_) join_data(std::move(f));
  auto *caladan = get_runtime()->caladan();
  if (head) {
    caladan->thread_ready_head(th);
  } else {
    caladan->thread_ready(th);
  }
}

uint64_t Thread::get_current_id() {
  auto *proclet_header = get_runtime()->get_current_proclet_header();

  if (proclet_header) {
    return get_runtime()->get_proclet_stack_range(Caladan::thread_self()).end;
  } else {
    return get_runtime()->caladan()->get_current_thread_id();
  }
}

}  // namespace nu

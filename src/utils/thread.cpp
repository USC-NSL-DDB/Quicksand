#include "nu/utils/thread.hpp"

namespace nu {

__attribute__((optimize("no-omit-frame-pointer"))) void
Thread::trampoline_in_obj_env(void *args) {
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

  {
    rt::Preempt p;
    rt::PreemptGuard g(&p);

    thread_unset_owner_heap();
  }

  auto runtime_stack_base = thread_get_runtime_stack_base();
  auto old_rsp = switch_stack(runtime_stack_base);
  Runtime::switch_to_runtime_slab();

  // auto *heap_header = d->header;
  if (likely(thread_is_at_creator())) {
    auto obj_stack_addr =
        ((reinterpret_cast<uintptr_t>(old_rsp) + kStackSize - 1) &
         (~(kStackSize - 1)));
    Runtime::stack_manager->put(reinterpret_cast<uint8_t *>(obj_stack_addr));
  } else {
    // FIXME
    // heap_header->migrated_wg.Done();
  }
  rt::Exit();
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
    thread_set_nu_thread(th_, nullptr);
    return;
  }

  join_data_->done = true;
  join_data_->cv.wait_and_unlock(&join_data_->lock);
  join_data_ = nullptr;
  thread_set_nu_thread(th_, nullptr);
}

void Thread::detach() {
  BUG_ON(!join_data_);

  join_data_->lock.lock();
  if (join_data_->done) {
    join_data_->cv.signal();
    join_data_->lock.unlock();
    join_data_ = nullptr;
    thread_set_nu_thread(th_, nullptr);
    return;
  }

  join_data_->done = true;
  join_data_->lock.unlock();
  join_data_ = nullptr;
  thread_set_nu_thread(th_, nullptr);
}

} // namespace nu

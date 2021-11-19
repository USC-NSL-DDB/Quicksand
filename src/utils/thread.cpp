#include "nu/utils/thread.hpp"

namespace nu {

__attribute__((noinline))
__attribute__((optimize("no-omit-frame-pointer"))) void
Thread::__trampoline_in_obj_env(join_data *d, HeapHeader *heap_header) {
  {
    OutermostMigrationDisabledGuard guard = std::move(d->guard);

    {
      MigrationEnabledGuard guard;
      d->func();
    }

    {
      RuntimeHeapGuard g;
      heap_header->threads->remove(thread_self());
    }
  }

  d->lock.lock();
  if (d->done) {
    d->cv.signal();
    d->lock.unlock();
  } else {
    d->done = true;
    d->cv.wait(&d->lock);
    d->lock.unlock();
    delete d;
  }

  if (unlikely(thread_is_migrated())) {
    heap_header->migrated_wg.Done();
    auto runtime_stack_base = thread_get_runtime_stack_base();
    switch_to_runtime_stack(runtime_stack_base);
    rt::Exit();
  }
}

__attribute__((optimize("no-omit-frame-pointer"))) void
Thread::trampoline_in_obj_env(void *args) {
  auto *d = *reinterpret_cast<join_data **>(args);  
  auto *heap_header = d->guard.get_heap_header();
  BUG_ON(!heap_header);
  heap_header->threads->put(thread_self());
  auto *obj_stack = Runtime::stack_manager->get();
  BUG_ON(reinterpret_cast<uintptr_t>(obj_stack) % kStackAlignment);
  auto &slab = heap_header->slab;
  Runtime::switch_to_obj_heap(&slab);
  auto *old_rsp = switch_to_obj_stack(obj_stack);

  __trampoline_in_obj_env(d, heap_header);

  switch_to_runtime_stack(old_rsp);
  Runtime::switch_to_runtime_heap();
  Runtime::stack_manager->put(obj_stack);
}

void Thread::trampoline_in_runtime_env(void *args) {
  auto *d = *reinterpret_cast<join_data **>(args);

  d->func();
  d->lock.lock();
  if (d->done) {
    d->cv.signal();
    d->lock.unlock();
  } else {
    d->done = true;
    d->cv.wait(&d->lock);
    d->lock.unlock();
    delete d;
  }
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
  join_data_->cv.wait(&join_data_->lock);
  join_data_->lock.unlock();
  delete join_data_;
  join_data_ = nullptr;
}
}

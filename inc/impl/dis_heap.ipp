extern "C" {
#include <base/compiler.h>
}

#include "utils/promise.hpp"

namespace nu {

inline DistributedHeap::FullShard::FullShard(uint32_t size,
                                             const RemObj<ErasedType>::Cap &cap)
    : failed_alloc_size(size), rem_obj(cap) {}

inline DistributedHeap::FullShard::FullShard(uint32_t size,
                                             RemObj<ErasedType> &&obj)
    : failed_alloc_size(size), rem_obj(std::move(obj)) {}

inline DistributedHeap::FullShard::FullShard(FullShard &&o)
    : failed_alloc_size(o.failed_alloc_size), rem_obj(std::move(o.rem_obj)) {}

inline DistributedHeap::FullShard &
DistributedHeap::FullShard::operator=(FullShard &&o) {
  failed_alloc_size = o.failed_alloc_size;
  rem_obj = std::move(o.rem_obj);
  return *this;
}

inline DistributedHeap::DistributedHeap()
    : last_probing_us_(microtime()), probing_active_(false), done_(false) {}

inline DistributedHeap::DistributedHeap(DistributedHeap &&o)
    : last_probing_us_(microtime()), probing_active_(false), done_(false) {
  *this = std::move(o);
}

inline DistributedHeap &DistributedHeap::operator=(DistributedHeap &&o) {
  o.halt_probing();
  free_shards_ = std::move(o.free_shards_);
  full_shards_ = std::move(o.full_shards_);
  last_probing_us_ = o.last_probing_us_;
  return *this;
}

inline DistributedHeap::~DistributedHeap() { halt_probing(); }

inline void DistributedHeap::halt_probing() {
  {
    rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
    done_ = true;
  }
  if (probing_active_) {
    probing_thread_.Join();
  }
}

template <typename T, typename... As>
RemPtr<T> DistributedHeap::allocate(As &&... args) {
retry:
  probing_mutex_.Lock();
  if (unlikely(free_shards_.empty())) {
    free_shards_.emplace_back(std::move(RemObj<ErasedType>::create()));
  }
  auto &free_shard = free_shards_.front();
  probing_mutex_.Unlock();
  auto *allocate_fn = +[](ErasedType &, As &... args) {
    auto *heap_header = Runtime::get_current_obj_heap_header();
    auto *obj_space = heap_header->slab.allocate(sizeof(T));
    if (unlikely(!obj_space)) {
      return RemPtr<T>();
    }
    new (obj_space) T(args...);
    return to_rem_ptr(reinterpret_cast<T *>(obj_space));
  };
  auto rem_ptr = free_shard.__run(allocate_fn, args...);
  if (!rem_ptr) {
    rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
    full_shards_.emplace_back(sizeof(T), std::move(free_shard));
    free_shards_.pop_front();
    goto retry;
  }
  return rem_ptr;
}

template <typename T, typename... As>
Future<RemPtr<T>> DistributedHeap::allocate_async(As &&... args) {
  auto *promise =
      Promise<T>::create([&, args...] { return allocate(args...); });
  return promise->get_future();
}

template <typename T> void DistributedHeap::free(const RemPtr<T> &ptr) {
  auto *free_fn = +[](ErasedType &, void *raw_ptr) {
    auto *heap_header = Runtime::get_current_obj_heap_header();
    heap_header->slab.free(raw_ptr);
  };
  const_cast<RemPtr<T> *>(&ptr)->rem_obj_.__run(free_fn, ptr.raw_ptr_);
}

template <typename T>
Future<void> DistributedHeap::free_async(const RemPtr<T> &ptr) {
  auto *promise = Promise<T>::create([&] { free(ptr); });
  return promise->get_future();
}

inline void DistributedHeap::check_probing() {
  auto cur_us = microtime();
  if (unlikely(cur_us >
               last_probing_us_ + kFullShardProbingIntervalMs * 1000)) {
    if (likely(!done_)) {
      __check_probing(cur_us);
    }
  }
}

} // namespace nu

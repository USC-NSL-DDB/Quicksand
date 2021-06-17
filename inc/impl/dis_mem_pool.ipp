#include <cereal/types/deque.hpp>

extern "C" {
#include <base/compiler.h>
}

#include "utils/promise.hpp"

namespace nu {

inline DistributedMemPool::Shard::Shard(uint32_t shard_size) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_header->slab.try_shrink(shard_size));
}

template <typename T, typename... As>
RemRawPtr<T> DistributedMemPool::Shard::allocate(As &&... args) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  auto *obj_space = heap_header->slab.allocate(sizeof(T));
  if (unlikely(!obj_space)) {
    return RemRawPtr<T>();
  }
  new (obj_space) T(args...);
  return to_rem_raw_ptr(reinterpret_cast<T *>(obj_space));
}

template <typename T> void DistributedMemPool::Shard::free(T *raw_ptr) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  heap_header->slab.free(raw_ptr);
}

inline bool DistributedMemPool::Shard::has_space_for(uint32_t size) {
  auto &slab = Runtime::get_current_obj_heap_header()->slab;
  auto *buf = slab.allocate(size);
  if (!buf) {
    return false;
  }
  slab.free(buf);
  return true;
}

inline DistributedMemPool::FullShard::FullShard() {}

inline DistributedMemPool::FullShard::FullShard(uint32_t size,
                                                RemObj<Shard> &&obj)
    : failed_alloc_size(size), rem_obj(std::move(obj)) {}

inline DistributedMemPool::FullShard::FullShard(FullShard &&o)
    : failed_alloc_size(o.failed_alloc_size), rem_obj(std::move(o.rem_obj)) {}

inline DistributedMemPool::FullShard &
DistributedMemPool::FullShard::operator=(FullShard &&o) {
  failed_alloc_size = o.failed_alloc_size;
  rem_obj = std::move(o.rem_obj);
  return *this;
}

inline DistributedMemPool::DistributedMemPool()
    : last_probing_us_(microtime()), probing_active_(false), done_(false) {}

inline DistributedMemPool::DistributedMemPool(DistributedMemPool &&o)
    : last_probing_us_(microtime()), probing_active_(false), done_(false) {
  *this = std::move(o);
}

inline DistributedMemPool &
DistributedMemPool::operator=(DistributedMemPool &&o) {
  o.halt_probing();
  free_shards_ = std::move(o.free_shards_);
  full_shards_ = std::move(o.full_shards_);
  last_probing_us_ = o.last_probing_us_;
  return *this;
}

inline DistributedMemPool::~DistributedMemPool() { halt_probing(); }

inline void DistributedMemPool::halt_probing() {
  {
    rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
    done_ = true;
  }
  if (probing_active_) {
    probing_thread_.Join();
  }
}

template <typename T, typename... As>
RemRawPtr<T> DistributedMemPool::allocate(As &&... args) {
retry:
  probing_mutex_.Lock();
  if (unlikely(free_shards_.empty())) {
    free_shards_.emplace_back(std::move(RemObj<Shard>::create(kShardSize)));
  }
  auto &free_shard = free_shards_.front();
  probing_mutex_.Unlock();
  auto rem_raw_ptr = free_shard.__run(&Shard::allocate<T, As...>, args...);
  if (!rem_raw_ptr) {
    rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
    full_shards_.emplace_back(sizeof(T), std::move(free_shard));
    free_shards_.pop_front();
    goto retry;
  }
  return rem_raw_ptr;
}

template <typename T, typename... As>
Future<RemRawPtr<T>> DistributedMemPool::allocate_async(As &&... args) {
  auto *promise =
      Promise<T>::create([&, args...] { return allocate(args...); });
  return promise->get_future();
}

template <typename T> void DistributedMemPool::free(const RemRawPtr<T> &ptr) {
  RemObj<Shard> shard(ptr.rem_obj_id_, false);
  shard.__run(&Shard::free<T>, const_cast<RemRawPtr<T> &>(ptr).get());
}

template <typename T>
Future<void> DistributedMemPool::free_async(const RemRawPtr<T> &ptr) {
  auto *promise = Promise<T>::create([&] { free(ptr); });
  return promise->get_future();
}

inline void DistributedMemPool::check_probing() {
  auto cur_us = microtime();
  if (unlikely(cur_us >
               last_probing_us_ + kFullShardProbingIntervalMs * 1000)) {
    if (likely(!done_)) {
      __check_probing(cur_us);
    }
  }
}

template <class Archive> void DistributedMemPool::save(Archive &ar) const {
  const_cast<DistributedMemPool *>(this)->save(ar);
}

template <class Archive> void DistributedMemPool::save(Archive &ar) {
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  ar(free_shards_, full_shards_);
}

template <class Archive> void DistributedMemPool::load(Archive &ar) {
  ar(free_shards_, full_shards_);

  last_probing_us_ = microtime();
  probing_active_ = false;
  done_ = false;
}

} // namespace nu

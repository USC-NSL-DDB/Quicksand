#include <cereal/types/deque.hpp>

extern "C" {
#include <base/compiler.h>
}

#include "rem_raw_ptr.hpp"
#include "rem_unique_ptr.hpp"
#include "utils/promise.hpp"

namespace nu {

inline DistributedMemPool::Shard::Shard(uint32_t shard_size) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_header->slab.try_shrink(shard_size));
}

template <typename T, typename... As>
RemRawPtr<T> DistributedMemPool::Shard::allocate_raw(As &&... args) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  auto *obj_space = heap_header->slab.allocate(sizeof(T));
  if (unlikely(!obj_space)) {
    return RemRawPtr<T>();
  }

  new (obj_space) T(args...);
  return RemRawPtr(reinterpret_cast<T *>(obj_space));
}

template <typename T, typename... As>
RemUniquePtr<T> DistributedMemPool::Shard::allocate_unique(As &&... args) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  auto *obj_space = heap_header->slab.allocate(sizeof(T));
  if (unlikely(!obj_space)) {
    return RemUniquePtr<T>();
  }

  std::unique_ptr<T> unique_ptr(new (obj_space) T(args...));
  return RemUniquePtr<T>(unique_ptr);
}

template <typename T> void DistributedMemPool::Shard::free_raw(T *raw_ptr) {
  raw_ptr->~T();
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
RemRawPtr<T> DistributedMemPool::allocate_raw(As &&... args) {
retry:
  auto free_shard = atomic_pick_free_shard();
  auto rem_raw_ptr = free_shard.__run(&Shard::allocate_raw<T, As...>, args...);
  if (!rem_raw_ptr) {
    atomic_put_full_shard(sizeof(T), std::move(free_shard));
    goto retry;
  } else {
    atomic_put_free_shard(std::move(free_shard));
  }
  return rem_raw_ptr;
}

template <typename T, typename... As>
Future<RemRawPtr<T>> DistributedMemPool::allocate_raw_async(As &&... args) {
  auto *promise =
      Promise<T>::create([&, args...] { return allocate_raw(args...); });
  return promise->get_future();
}

template <typename T>
void DistributedMemPool::free_raw(const RemRawPtr<T> &ptr) {
  RemObj<Shard> shard(ptr.rem_obj_id_, false);
  shard.__run(&Shard::free_raw<T>, const_cast<RemRawPtr<T> &>(ptr).get());
}

template <typename T>
Future<void> DistributedMemPool::free_raw_async(const RemRawPtr<T> &ptr) {
  auto *promise = Promise<T>::create([&] { free_raw(ptr); });
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

template <typename T, typename... As>
RemUniquePtr<T> DistributedMemPool::allocate_unique(As &&... args) {
retry:
  auto free_shard = atomic_pick_free_shard();
  auto rem_unique_ptr =
      free_shard.__run(&Shard::allocate_unique<T, As...>, args...);
  if (!rem_unique_ptr) {
    atomic_put_full_shard(sizeof(T), std::move(free_shard));
    goto retry;
  } else {
    atomic_put_free_shard(std::move(free_shard));
  }
  return rem_unique_ptr;
}

template <typename T, typename... As>
Future<RemUniquePtr<T>>
DistributedMemPool::allocate_unique_async(As &&... args) {
  auto *promise =
      Promise<T>::create([&, args...] { return allocate_unique(args...); });
  return promise->get_future();
}

} // namespace nu

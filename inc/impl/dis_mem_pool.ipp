#include <cereal/types/deque.hpp>

extern "C" {
#include <base/compiler.h>
}

#include "rem_raw_ptr.hpp"
#include "rem_shared_ptr.hpp"
#include "rem_unique_ptr.hpp"
#include "utils/promise.hpp"

namespace nu {

inline DistributedMemPool::Shard::Shard(uint32_t shard_size) {
  auto *heap_header = Runtime::get_current_obj_heap_header();
  BUG_ON(!heap_header->slab.try_shrink(shard_size));
}

template <typename T, typename... As>
RemRawPtr<T> DistributedMemPool::Shard::allocate_raw(As &&... args) {
  return RemRawPtr(new T(std::forward<As>(args)...));
}

template <typename T, typename... As>
RemUniquePtr<T> DistributedMemPool::Shard::allocate_unique(As &&... args) {
  return make_rem_unique<T>(std::forward<As>(args)...);
}

template <typename T, typename... As>
RemSharedPtr<T> DistributedMemPool::Shard::allocate_shared(As &&... args) {
  return make_rem_shared<T>(std::forward<As>(args)...);
}

template <typename T> void DistributedMemPool::Shard::free_raw(T *raw_ptr) {
  delete raw_ptr;
}

inline bool DistributedMemPool::Shard::has_space_for(uint32_t size) {
  auto buf = new uint8_t[size];
  if (!buf) {
    return false;
  }
  delete[] buf;
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
  return general_allocate<T>(&Shard::allocate_raw<T, As...>,
                             std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<RemRawPtr<T>> DistributedMemPool::allocate_raw_async(As &&... args) {
  auto *promise = Promise<T>::create(
      [&, args...] { return allocate_raw(std::forward<As>(args)...); });
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
  return general_allocate<T>(&Shard::allocate_unique<T, As...>,
                             std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<RemUniquePtr<T>>
DistributedMemPool::allocate_unique_async(As &&... args) {
  auto *promise = Promise<T>::create(
      [&, args...] { return allocate_unique(std::forward<As>(args)...); });
  return promise->get_future();
}

template <typename T, typename... As>
RemSharedPtr<T> DistributedMemPool::allocate_shared(As &&... args) {
  return general_allocate<T>(&Shard::allocate_shared<T, As...>,
                             std::forward<As>(args)...);
}

template <typename T, typename... As>
Future<RemSharedPtr<T>>
DistributedMemPool::allocate_shared_async(As &&... args) {
  auto *promise = Promise<T>::create(
      [&, args...] { return allocate_shared(std::forward<As>(args)...); });
  return promise->get_future();
}

template <typename T, typename AllocFn, typename... As>
auto DistributedMemPool::general_allocate(AllocFn &&alloc_fn, As &&... args) {
retry:
  auto free_shard = atomic_pick_free_shard();
  auto ptr = free_shard.__run(alloc_fn, std::forward<As>(args)...);
  if (!ptr) {
    atomic_put_full_shard(sizeof(T), std::move(free_shard));
    goto retry;
  } else {
    atomic_put_free_shard(std::move(free_shard));
  }
  return ptr;
}

} // namespace nu

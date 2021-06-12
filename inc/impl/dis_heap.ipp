#include "utils/promise.hpp"

namespace nu {

DistributedHeap::DistributedHeap() {}

DistributedHeap::DistributedHeap(const Cap &cap) {
  for (auto &cap : cap.free_shard_caps) {
    free_shards_.emplace_back(cap);
  }
  for (auto &cap : cap.full_shard_caps) {
    full_shards_.emplace_back(cap);
  }
}

DistributedHeap::DistributedHeap(DistributedHeap &&o) { *this = std::move(o); }

DistributedHeap &DistributedHeap::operator=(DistributedHeap &&o) {
  for (auto &shard : o.free_shards_) {
    free_shards_.emplace_back(std::move(shard));
  }
  for (auto &shard : o.full_shards_) {
    full_shards_.emplace_back(std::move(shard));
  }
  o.free_shards_.clear();
  o.full_shards_.clear();
  return *this;
}

DistributedHeap::Cap DistributedHeap::get_cap() {
  Cap cap;
  for (auto &shard : free_shards_) {
    cap.free_shard_caps.emplace_back(shard.get_cap());
  }
  for (auto &shard : full_shards_) {
    cap.full_shard_caps.emplace_back(shard.get_cap());
  }
  return cap;
}

template <typename T, typename... As>
RemPtr<T> DistributedHeap::allocate(As &&... args) {
retry:
  if (unlikely(free_shards_.empty())) {
    free_shards_.emplace_back(std::move(RemObj<ErasedType>::create()));
  }
  auto &free_shard = free_shards_.front();
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
    full_shards_.emplace_back(std::move(free_shard));
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
} // namespace nu

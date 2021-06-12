#pragma once

#include <cereal/types/vector.hpp>
#include <list>
#include <vector>

#include <sync.h>

#include "rem_obj.hpp"
#include "rem_ptr.hpp"
#include "utils/future.hpp"

namespace nu {

// TODO: make it thread-safe.
// TODO: add batch interface.
// TODO: periodically check if a full shard is now a free shard.
class DistributedHeap {
public:
  struct Cap {
    std::vector<RemObj<ErasedType>::Cap> free_shard_caps;
    std::vector<RemObj<ErasedType>::Cap> full_shard_caps;

    template <class Archive> void serialize(Archive &ar) {
      ar(free_shard_caps, full_shard_caps);
    }
  };

  DistributedHeap();
  DistributedHeap(const Cap &cap);
  DistributedHeap(const DistributedHeap &) = delete;
  DistributedHeap &operator=(const DistributedHeap &) = delete;
  DistributedHeap(DistributedHeap &&);
  DistributedHeap &operator=(DistributedHeap &&);
  Cap get_cap();
  template <typename T, typename... As> RemPtr<T> allocate(As &&... args);
  template <typename T, typename... As>
  Future<RemPtr<T>> allocate_async(As &&... args);
  template <typename T> void free(const RemPtr<T> &ptr);
  template <typename T> Future<void> free_async(const RemPtr<T> &ptr);

private:
  std::list<RemObj<ErasedType>> free_shards_;
  std::list<RemObj<ErasedType>> full_shards_;
};

} // namespace nu

#include "impl/dis_heap.ipp"

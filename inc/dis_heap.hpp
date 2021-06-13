#pragma once

#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <deque>
#include <utility>
#include <vector>

#include <sync.h>
#include <thread.h>
extern "C" {
#include <base/time.h>
}

#include "rem_obj.hpp"
#include "rem_ptr.hpp"
#include "utils/future.hpp"

namespace nu {

// TODO: make it thread-safe.
// TODO: add batch interface.
// TODO: configurable shard size.
class DistributedHeap {
public:
  constexpr static uint32_t kFullShardProbingIntervalMs = 400;

  struct Cap {
    std::vector<RemObj<ErasedType>::Cap> free_shard_caps;
    std::vector<std::pair<uint32_t, RemObj<ErasedType>::Cap>> full_shard_infos;

    template <class Archive> void serialize(Archive &ar) {
      ar(free_shard_caps, full_shard_infos);
    }
  };

  DistributedHeap();
  DistributedHeap(const Cap &cap);
  DistributedHeap(const DistributedHeap &) = delete;
  DistributedHeap &operator=(const DistributedHeap &) = delete;
  DistributedHeap(DistributedHeap &&);
  DistributedHeap &operator=(DistributedHeap &&);
  ~DistributedHeap();
  Cap get_cap();
  template <typename T, typename... As> RemPtr<T> allocate(As &&... args);
  template <typename T, typename... As>
  Future<RemPtr<T>> allocate_async(As &&... args);
  template <typename T> void free(const RemPtr<T> &ptr);
  template <typename T> Future<void> free_async(const RemPtr<T> &ptr);

private:
  struct FullShard {
    uint32_t failed_alloc_size;
    RemObj<ErasedType> rem_obj;

    FullShard(uint32_t failed_alloc_size, RemObj<ErasedType> &&obj);
    FullShard(uint32_t failed_alloc_size, const RemObj<ErasedType>::Cap &cap);
    FullShard(FullShard &&o);
    FullShard &operator=(FullShard &&o);
  };

  std::deque<RemObj<ErasedType>> free_shards_;
  std::deque<FullShard> full_shards_;

  uint64_t last_probing_us_;
  rt::Thread probing_thread_;
  bool probing_active_;
  rt::Mutex probing_mutex_;

  bool done_;

  void check_probing();
  void __check_probing(uint64_t cur_us);
  void probing_fn();
  void halt_probing();
};

} // namespace nu

#include "impl/dis_heap.ipp"

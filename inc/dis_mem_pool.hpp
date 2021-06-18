#pragma once

#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include <sync.h>
#include <thread.h>
extern "C" {
#include <base/time.h>
}

#include "rem_obj.hpp"
#include "utils/future.hpp"

namespace nu {

template <typename T> class RemRawPtr;
template <typename T> class RemUniquePtr;

// TODO: make it thread-safe.
// TODO: add batch interface.
class DistributedMemPool {
public:
  constexpr static uint32_t kFullShardProbingIntervalMs = 400;
  constexpr static uint32_t kShardSize = 2 << 20;

  DistributedMemPool();
  DistributedMemPool(const DistributedMemPool &) = delete;
  DistributedMemPool &operator=(const DistributedMemPool &) = delete;
  DistributedMemPool(DistributedMemPool &&);
  DistributedMemPool &operator=(DistributedMemPool &&);
  ~DistributedMemPool();
  template <typename T, typename... As>
  RemRawPtr<T> allocate_raw(As &&... args);
  template <typename T, typename... As>
  Future<RemRawPtr<T>> allocate_raw_async(As &&... args);
  template <typename T, typename... As>
  RemUniquePtr<T> allocate_unique(As &&... args);
  template <typename T, typename... As>
  Future<RemUniquePtr<T>> allocate_unique_async(As &&... args);
  template <typename T> void free_raw(const RemRawPtr<T> &ptr);
  template <typename T> Future<void> free_raw_async(const RemRawPtr<T> &ptr);

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void save(Archive &ar);
  template <class Archive> void load(Archive &ar);

private:
  struct Shard {
    Shard(uint32_t shard_size);
    template <typename T, typename... As>
    RemRawPtr<T> allocate_raw(As &&... args);
    template <typename T, typename... As>
    RemUniquePtr<T> allocate_unique(As &&... args);
    template <typename T> void free_raw(T *raw_ptr);
    bool has_space_for(uint32_t size);
  };

  struct FullShard {
    uint32_t failed_alloc_size;
    RemObj<Shard> rem_obj;

    FullShard();
    FullShard(uint32_t failed_alloc_size, RemObj<Shard> &&obj);
    FullShard(FullShard &&o);
    FullShard &operator=(FullShard &&o);

    template <class Archive> void serialize(Archive &ar) {
      ar(failed_alloc_size, rem_obj);
    }
  };

  using FreeShard = RemObj<Shard>;

  std::deque<FreeShard> free_shards_;
  std::deque<FullShard> full_shards_;

  uint64_t last_probing_us_;
  rt::Thread probing_thread_;
  bool probing_active_;
  rt::Mutex probing_mutex_;

  bool done_;

  template <typename T> friend class RemUniquePtr;

  FreeShard atomic_pick_free_shard();
  void atomic_put_free_shard(FreeShard &&free_shard);
  void atomic_put_full_shard(uint32_t failed_alloc_size,
                             FreeShard &&free_shard);
  void check_probing();
  void __check_probing(uint64_t cur_us);
  void probing_fn();
  void halt_probing();
};

} // namespace nu

#include "impl/dis_mem_pool.ipp"

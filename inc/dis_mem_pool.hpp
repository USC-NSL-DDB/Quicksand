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
class DistributedMemPool {
public:
  constexpr static uint32_t kFullShardProbingIntervalMs = 400;
  constexpr static uint32_t kShardSize = 2 << 20;

  struct Shard {
    Shard(uint32_t shard_size);
    template <typename T, typename... As> RemPtr<T> allocate(As &&... args);
    template <typename T> void free(T *raw_ptr);
    bool has_space_for(uint32_t size);
  };

  DistributedMemPool();
  DistributedMemPool(const DistributedMemPool &) = delete;
  DistributedMemPool &operator=(const DistributedMemPool &) = delete;
  DistributedMemPool(DistributedMemPool &&);
  DistributedMemPool &operator=(DistributedMemPool &&);
  ~DistributedMemPool();
  template <typename T, typename... As> RemPtr<T> allocate(As &&... args);
  template <typename T, typename... As>
  Future<RemPtr<T>> allocate_async(As &&... args);
  template <typename T> void free(const RemPtr<T> &ptr);
  template <typename T> Future<void> free_async(const RemPtr<T> &ptr);

  template <class Archive> void save(Archive &ar) const;
  template <class Archive> void save(Archive &ar);
  template <class Archive> void load(Archive &ar);

private:
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

  std::deque<RemObj<Shard>> free_shards_;
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

#include "impl/dis_mem_pool.ipp"

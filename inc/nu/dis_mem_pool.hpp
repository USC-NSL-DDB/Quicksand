#pragma once

#include <cereal/types/optional.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

extern "C" {
#include <base/time.h>
}
#include <sync.h>
#include <thread.h>

#include "nu/commons.hpp"
#include "nu/proclet.hpp"
#include "nu/utils/future.hpp"

namespace nu {

template <typename T>
class RemRawPtr;
template <typename T>
class RemUniquePtr;
template <typename T>
class RemSharedPtr;

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
  template <typename T, typename... As>
  RemSharedPtr<T> allocate_shared(As &&... args);
  template <typename T, typename... As>
  Future<RemSharedPtr<T>> allocate_shared_async(As &&... args);
  template <typename T>
  void free_raw(const RemRawPtr<T> &ptr);
  template <typename T>
  Future<void> free_raw_async(const RemRawPtr<T> &ptr);

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void save(Archive &ar);
  template <class Archive>
  void load(Archive &ar);

 private:
  struct Heap {
    Heap(uint32_t shard_size);
    template <typename T, typename... As>
    RemRawPtr<T> allocate_raw(As... args);
    template <typename T, typename... As>
    RemUniquePtr<T> allocate_unique(As... args);
    template <typename T, typename... As>
    RemSharedPtr<T> allocate_shared(As... args);
    template <typename T>
    void free_raw(T *raw_ptr);
    bool has_space_for(uint32_t size);
  };

  struct Shard {
    uint32_t failed_alloc_size = 0;
    Proclet<Heap> proclet;

    Shard();
    Shard(Proclet<Heap> &&proclet);
    Shard(Shard &&o);
    Shard &operator=(Shard &&o);

    template <class Archive>
    void serialize(Archive &ar) {
      ar(failed_alloc_size, proclet);
    }
  };

  struct alignas(kCacheLineBytes) FreeShardPerCoreCache {
    std::optional<Shard> shard;

    template <class Archive>
    void serialize(Archive &ar) {
      ar(shard);
    }
  };

  FreeShardPerCoreCache local_free_shards_[kNumCores];
  std::deque<Shard> global_free_shards_;
  std::deque<Shard> global_full_shards_;
  rt::Mutex global_mutex_;
  uint64_t last_probing_us_;
  std::unique_ptr<rt::Thread> probing_thread_;
  bool probing_active_;

  bool done_;

  template <typename T>
  friend class RemUniquePtr;

  template <typename T, typename AllocFn, typename... As>
  auto __allocate(AllocFn &&alloc_fn, As &&... args);
  void __handle_local_free_shard_full();
  void __handle_no_local_free_shard();
  void check_probing();
  void __check_probing(uint64_t cur_us);
  void probing_fn();
  void halt_probing();
};

}  // namespace nu

#include "nu/impl/dis_mem_pool.ipp"

#include "dis_mem_pool.hpp"

namespace nu {

void DistributedMemPool::__check_probing(uint64_t cur_us) {
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  if (likely(!probing_active_ && !done_)) {
    last_probing_us_ = cur_us;
    probing_active_ = true;
    probing_thread_.Join();
    probing_thread_ = std::move(rt::Thread([&] { probing_fn(); }));
  }
}

void DistributedMemPool::probing_fn() {
  auto num_probes = full_shards_.size();
  while (num_probes-- && !ACCESS_ONCE(done_)) {
    probing_mutex_.Lock();
    auto full_shard = std::move(full_shards_.front());
    full_shards_.pop_front();
    probing_mutex_.Unlock();
    if (full_shard.rem_obj.run(&Shard::has_space_for,
                               full_shard.failed_alloc_size)) {
      rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
      free_shards_.emplace_back(std::move(full_shard.rem_obj));
    }
  }
  probing_active_ = false;
}

DistributedMemPool::FreeShard DistributedMemPool::atomic_pick_free_shard() {
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  if (unlikely(free_shards_.empty())) {
    free_shards_.emplace_back(std::move(RemObj<Shard>::create(kShardSize)));
  }
  auto free_shard = std::move(free_shards_.front());
  free_shards_.pop_front();
  return free_shard;
}

void DistributedMemPool::atomic_put_free_shard(FreeShard &&free_shard) {
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  free_shards_.emplace_front(std::move(free_shard));
}

void DistributedMemPool::atomic_put_full_shard(uint32_t failed_alloc_size,
                                               FreeShard &&free_shard) {
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  full_shards_.emplace_back(failed_alloc_size, std::move(free_shard));
}

} // namespace nu

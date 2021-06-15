#include "dis_heap.hpp"

namespace nu {

void DistributedHeap::__check_probing(uint64_t cur_us) {
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  if (likely(!probing_active_ && !done_)) {
    last_probing_us_ = cur_us;
    probing_active_ = true;
    probing_thread_.Join();
    probing_thread_ = std::move(rt::Thread([&] { probing_fn(); }));
  }
}

void DistributedHeap::probing_fn() {
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

} // namespace nu

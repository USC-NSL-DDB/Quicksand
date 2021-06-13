#include "dis_heap.hpp"

namespace nu {

DistributedHeap::DistributedHeap(const Cap &cap)
    : last_probing_us_(microtime()), probing_active_(false), done_(false) {
  for (auto &cap : cap.free_shard_caps) {
    free_shards_.emplace_back(cap);
  }
  for (auto &info : cap.full_shard_infos) {
    full_shards_.emplace_back(info.first, info.second);
  }
}

DistributedHeap::Cap DistributedHeap::get_cap() {
  Cap cap;
  rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
  for (auto &shard : free_shards_) {
    cap.free_shard_caps.emplace_back(shard.get_cap());
  }
  for (auto &info : full_shards_) {
    cap.full_shard_infos.emplace_back(info.failed_alloc_size,
                                      info.rem_obj.get_cap());
  }
  return cap;
}

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

    auto *try_alloc_fn = +[](ErasedType &, uint32_t failed_alloc_size) {
      auto &slab = Runtime::get_current_obj_heap_header()->slab;
      auto *buf = slab.allocate(failed_alloc_size);
      if (!buf) {
        return false;
      }
      slab.free(buf);
      return true;
    };
    if (full_shard.rem_obj.run(try_alloc_fn, full_shard.failed_alloc_size)) {
      rt::ScopedLock<rt::Mutex> scope(&probing_mutex_);
      free_shards_.emplace_back(std::move(full_shard.rem_obj));
    }
  }
  probing_active_ = false;
}

} // namespace nu

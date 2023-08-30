#pragma once

#include <array>
#include <atomic>
#include <queue>
#include <sync.h>
#include <nu/sharded_queue.hpp>
#include <nu/utils/time.hpp>

namespace imagenet {

using GPUStatusType = uint8_t;

template <typename Item>
class MockGPU {
 public:
  using Traces = std::vector<std::vector<std::pair<uint64_t, uint64_t>>>;
  static constexpr uint32_t kImageProcessTimeUs = 200;
  static constexpr uint32_t kMaxNumImages = 300'000;

  MockGPU(nu::ShardedQueue<Item, std::true_type> queue, std::size_t max_gpus)
      : done_(false), num_gpus_(max_gpus) {
    all_traces_.reserve(max_gpus);
    for (std::size_t i = 0; i < max_gpus; ++i) {
      all_traces_.emplace_back();
      all_traces_.back().reserve(kMaxNumImages);
      ths_.emplace_back([this, queue, id = i] { gpu_fn(id, queue); });
    }
  }

  void gpu_fn(std::size_t id,
              nu::ShardedQueue<Item, std::true_type> remote_queue) {
    auto &traces = all_traces_[id];

    while (!nu::Caladan::access_once(done_)) {
      if (unlikely(id >= nu::Caladan::access_once(num_gpus_))) {
        nu::ScopedLock g(&spin_);
        while (id >= nu::Caladan::access_once(num_gpus_)) {
          cv_.wait(&spin_);
        }
      }

      auto popped = remote_queue.try_pop(1);
      if (!popped.empty()) {
	auto cur_us = microtime();
        auto last_done_us = traces.size() ? traces.back().second : 0;
        uint64_t start_us;
        if (cur_us >= last_done_us) {
          start_us = cur_us;
        } else {
          delay_us(last_done_us - cur_us);
          start_us = last_done_us;
        }
        traces.emplace_back(start_us, start_us + kImageProcessTimeUs);
      }
    }
  }

  Traces drain_and_stop() {
    done_ = true;
    barrier();
    set_num_gpus(ths_.size());
    barrier();
    for (auto &th : ths_) {
      th.Join();
    }
    return all_traces_;
  }

  void set_num_gpus(std::size_t num_gpus) {
    nu::ScopedLock g(&spin_);
    num_gpus_ = num_gpus;
    cv_.signal_all();
  }

  static void process(Item &item) { nu::Time::sleep(kImageProcessTimeUs); }

 private:
  bool done_;
  std::vector<rt::Thread> ths_;
  Traces all_traces_;
  nu::SpinLock spin_;
  nu::CondVar cv_;
  std::size_t num_gpus_;
};

}  // namespace imagenet

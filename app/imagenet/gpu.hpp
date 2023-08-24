#pragma once

#include <array>
#include <atomic>
#include <queue>
#include <sync.h>
#include <nu/sharded_queue.hpp>
#include <nu/utils/time.hpp>

namespace imagenet {

enum GPUStatus { kRunning = 0, kDrain, kPause };

using GPUStatusType = uint8_t;

template <typename Item>
class MockGPU {
 public:
  static constexpr uint32_t kImageProcessTimeUs = 100;
  static constexpr uint32_t kMaxNumImages = 300'000;

  MockGPU(nu::ShardedQueue<Item, std::true_type> queue, std::size_t max_gpus)
      : num_gpus_(max_gpus) {
    // all_traces_.resize(max_gpus);
    // num_traces_.resize(max_gpus);

    for (std::size_t i = 0; i < max_gpus; ++i) {
      ths_.emplace_back([this, queue, id = i] { gpu_fn(id, queue); });
    }
  }

  void gpu_fn(std::size_t id,
              nu::ShardedQueue<Item, std::true_type> remote_queue) {
    nu::Future<void> fut;
    while (true) {
      if (unlikely(id >= nu::Caladan::access_once(num_gpus_))) {
        nu::ScopedLock g(&spin_);
        while (id >= nu::Caladan::access_once(num_gpus_)) {
          cv_.wait(&spin_);
        }
      }

      auto popped = remote_queue.try_pop(1);
      if (!popped.empty()) {
        if (fut) fut.get();
        fut = nu::async(
            [img = std::move(popped.front())]() mutable { process(img); });
      } else {
        auto status = load_acquire(&status_);
        if (unlikely(status == GPUStatus::kDrain)) {
          break;
        }
        nu::Time::sleep(100);
      }
    }
  }

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> drain_and_stop() {
    status_ = GPUStatus::kDrain;
    barrier();
    for (auto &th : ths_) {
      th.Join();
    }
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> all_traces;
    // for (auto [traces, size] : std::views::zip(all_traces_, num_traces_)) {
    //   all_traces.emplace_back(
    //       std::vector(traces.begin(), traces.begin() + size));
    // }
    return all_traces;
  }

  void set_num_gpus(std::size_t num_gpus) {
    nu::ScopedLock g(&spin_);
    num_gpus_ = num_gpus;
    cv_.signal_all();
  }

  static void process(Item &item) { nu::Time::sleep(kImageProcessTimeUs); }

 private:
  GPUStatusType status_ = GPUStatus::kRunning;
  std::vector<rt::Thread> ths_;
  // std::vector<std::array<std::pair<uint64_t, uint64_t>, kMaxNumImages>>
  //     all_traces_;
  // std::vector<std::size_t> num_traces_;
  nu::SpinLock spin_;
  nu::CondVar cv_;
  std::size_t num_gpus_;
};

}  // namespace imagenet

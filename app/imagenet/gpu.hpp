#pragma once

#include <array>
#include <queue>
#include <sync.h>
#include <atomic>

#include <nu/sharded_queue.hpp>
#include <nu/utils/time.hpp>

namespace imagenet {

enum GPUStatus { kRunning = 0, kDrain, kPause };

using GPUStatusType = uint8_t;

template <typename Item>
class MockGPU {
 public:
  static constexpr std::size_t kBatchSize = 16;
  static constexpr uint32_t kProcessDelayUs = 333;
  static constexpr uint32_t kMaxNumImages = 300000;

  MockGPU() = default;
  MockGPU(nu::ShardedQueue<Item, std::true_type> queue)
      : remote_queue_(std::move(queue)),
        num_traces_(0),
        fetcher_th_([this] { fetcher_fn(); }),
        processor_th_([this] { processor_fn(); }) {}
  void fetcher_fn() {
    while (true) {
      auto status = load_acquire(&status_);
      if (unlikely(status == GPUStatus::kPause)) {
        rt::Preempt p;
        rt::PreemptGuard g(&p);
        g.Park(&fetcher_waker_);
      }

      barrier();
      if (local_queue_.size() >= kBatchSize) {
	continue;
      }

      auto popped = remote_queue_.try_pop(kBatchSize);
      if (unlikely(status == GPUStatus::kDrain && popped.empty())) {
        break;
      }

      {
        rt::ScopedLock lock(&spin_);
        for (auto &p : popped) {
          local_queue_.emplace(std::move(p));
        }
      }
    }
  }
  void processor_fn() {
    while (true) {
      auto status = load_acquire(&status_);

      barrier();
      if (!local_queue_.empty()) {
        barrier();
        auto start_us = microtime();
        barrier();

        std::queue<Item> tmp_queue;
        {
          rt::ScopedLock lock(&spin_);
          std::swap(tmp_queue, local_queue_);
        }
        while (!tmp_queue.empty()) {
          auto front = std::move(tmp_queue.front());
          tmp_queue.pop();
          process(std::move(front));
        }

        barrier();
        auto end_us = microtime();
        barrier();

        traces_[num_traces_++] = std::make_pair(start_us, end_us);
      }

      if (unlikely(status == GPUStatus::kDrain)) {
        fetcher_th_.Join();
	break;
      }
    }
  }
  std::vector<std::pair<uint64_t, uint64_t>> drain_and_stop() {
    status_ = GPUStatus::kDrain;
    barrier();
    processor_th_.Join();

    return std::vector<std::pair<uint64_t, uint64_t>>(
        traces_.begin(), traces_.begin() + num_traces_);
  }
  void pause() { rt::access_once(status_) = GPUStatus::kPause; }
  void resume() {
    status_ = GPUStatus::kRunning;
    barrier();
    fetcher_waker_.Wake();
  }
  static void process(Item &&item) { nu::Time::delay(kProcessDelayUs); }

 private:
  GPUStatusType status_ = GPUStatus::kRunning;
  nu::ShardedQueue<Item, std::true_type> remote_queue_;
  std::queue<Item> local_queue_;
  std::size_t num_traces_;
  std::array<std::pair<uint64_t, uint64_t>, kMaxNumImages> traces_;
  rt::Spin spin_;
  rt::Thread fetcher_th_;
  rt::Thread processor_th_;
  rt::ThreadWaker fetcher_waker_;
};
}  // namespace imagenet

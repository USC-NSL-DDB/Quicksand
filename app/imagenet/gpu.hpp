#pragma once

#include <sync.h>

#include <array>
#include <atomic>
#include <nu/sharded_queue.hpp>
#include <nu/utils/time.hpp>
#include <queue>

namespace imagenet {

enum GPUStatus { kRunning = 0, kDrain, kPause };

using GPUStatusType = uint8_t;

template <typename Item>
class MockInlineFetchGPU {
 public:
  static constexpr uint32_t kProcessDelayUs = 333;
  static constexpr uint32_t kMaxNumImages = 300'000;

  MockInlineFetchGPU() = default;
  MockInlineFetchGPU(nu::ShardedQueue<Item, std::true_type> queue,
                     std::size_t max_gpus)
      : num_gpus_(max_gpus) {
    all_traces_.resize(max_gpus);
    num_traces_.resize(max_gpus);
    for (std::size_t i = 0; i < max_gpus; ++i) {
      gpu_ths_.emplace_back([this, queue, id = i] { gpu_fn(id, queue); });
    }
  }

  void gpu_fn(std::size_t id,
              nu::ShardedQueue<Item, std::true_type> remote_queue) {
    while (true) {
      if (id >= num_gpus_) {
        continue;
      }

      auto out = remote_queue.try_pop(1);
      if (!out.empty()) {
        process(std::move(out[0]));
      } else {
        auto status = load_acquire(&status_);
        if (unlikely(status == GPUStatus::kDrain)) {
          break;
        }
      }
    }
  }

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> drain_and_stop() {
    status_ = GPUStatus::kDrain;
    barrier();

    for (auto &gpu : gpu_ths_) {
      gpu.Join();
    }

    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> all_traces;
    for (auto [traces, size] : std::views::zip(all_traces_, num_traces_)) {
      all_traces.emplace_back(
          std::vector(traces.begin(), traces.begin() + size));
    }
    return all_traces;
  }

  void set_num_gpus(std::size_t num_gpus) { num_gpus_ = num_gpus; }

  static void process(Item &&item) { nu::Time::sleep(kProcessDelayUs); }

 private:
  GPUStatusType status_ = GPUStatus::kRunning;
  std::vector<rt::Thread> gpu_ths_;
  std::vector<std::array<std::pair<uint64_t, uint64_t>, kMaxNumImages>>
      all_traces_;
  std::vector<std::size_t> num_traces_;
  std::atomic<std::size_t> num_gpus_;
};

template <typename Item>
class MockPipelinedGPU {
 public:
  static constexpr uint32_t kProcessDelayUs = 333;
  static constexpr uint32_t kMaxLocalQueueDepth = 1;
  static constexpr uint32_t kBatchSize = 16;
  static constexpr uint32_t kMaxNumImages = 300'000;

  MockPipelinedGPU() = default;
  MockPipelinedGPU(nu::ShardedQueue<Item, std::true_type> queue,
                   std::size_t max_gpus)
      : num_gpus_(max_gpus) {
    all_traces_.resize(max_gpus);
    num_traces_.resize(max_gpus);
    spins_.resize(max_gpus);
    queues_.resize(max_gpus);

    for (std::size_t i = 0; i < max_gpus; ++i) {
      fetcher_ths_.emplace_back(
          [this, queue, id = i] { fetcher_fn(id, queue); });
    }
    for (std::size_t i = 0; i < max_gpus; ++i) {
      gpu_ths_.emplace_back([this, id = i] { gpu_fn(id); });
    }
  }

  void fetcher_fn(std::size_t id,
                  nu::ShardedQueue<Item, std::true_type> remote_queue) {
    auto &spin = spins_[id];
    auto &local_queue = queues_[id];

    while (true) {
      {
        rt::ScopedLock lock(&spin);
        if (local_queue.size() >= kMaxLocalQueueDepth) {
          continue;
        }
      }

      auto popped = remote_queue.try_pop(kBatchSize);
      if (!popped.empty()) {
        {
          rt::ScopedLock lock(&spin);
          for (auto &p : popped) {
            local_queue.emplace(std::move(p));
          }
        }
      } else {
        auto status = load_acquire(&status_);
        if (unlikely(status == GPUStatus::kDrain)) {
          break;
        }
        nu::Time::sleep(500);
      }
    }
  }

  void gpu_fn(std::size_t id) {
    auto &spin = spins_[id];
    auto &local_queue = queues_[id];

    while (true) {
      if (id >= num_gpus_) {
        continue;
      }

      bool empty = false;
      Item item;
      {
        rt::ScopedLock lock(&spin);
        empty = local_queue.empty();
        if (!empty) {
          item = std::move(local_queue.front());
          local_queue.pop();
        }
      }
      if (!empty) {
        process(std::move(item));
      } else {
        if (unlikely(!fetcher_ths_[id].Joinable())) {
          break;
        }
      }
    }
  }

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> drain_and_stop() {
    status_ = GPUStatus::kDrain;
    barrier();

    for (auto &fetcher : fetcher_ths_) {
      fetcher.Join();
    }
    for (auto &gpu : gpu_ths_) {
      gpu.Join();
    }

    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> all_traces;
    for (auto [traces, size] : std::views::zip(all_traces_, num_traces_)) {
      all_traces.emplace_back(
          std::vector(traces.begin(), traces.begin() + size));
    }
    return all_traces;
  }

  void set_num_gpus(std::size_t num_gpus) { num_gpus_ = num_gpus; }

  static void process(Item &&item) { nu::Time::sleep(kProcessDelayUs); }

 private:
  GPUStatusType status_ = GPUStatus::kRunning;
  std::deque<rt::Spin> spins_;
  std::vector<std::queue<Item>> queues_;
  std::vector<rt::Thread> gpu_ths_;
  std::vector<rt::Thread> fetcher_ths_;
  std::vector<std::array<std::pair<uint64_t, uint64_t>, kMaxNumImages>>
      all_traces_;
  std::vector<std::size_t> num_traces_;
  std::atomic<std::size_t> num_gpus_;
};

template <typename Item>
class MockGPU {
 public:
  static constexpr std::size_t kNumFetchers = 8;
  static constexpr std::size_t kBatchSize = 1;
  static constexpr std::size_t kMaxLocalQueueDepth = 32;
  static constexpr uint32_t kProcessDelayUs = 333;
  static constexpr uint32_t kMaxNumImages = 300'000;

  MockGPU() = default;
  MockGPU(nu::ShardedQueue<Item, std::true_type> queue, std::size_t max_gpus)
      : num_gpus_(max_gpus) {
    all_traces_.resize(max_gpus);
    num_traces_.resize(max_gpus);

    for (std::size_t i = 0; i < kNumFetchers; ++i) {
      fetchers_ths_.emplace_back([this, queue, i] { fetcher_fn(queue, i); });
    }
    for (std::size_t i = 0; i < max_gpus; ++i) {
      gpu_ths_.emplace_back([this, queue, id = i] { gpu_fn(id, queue); });
    }
  }

  void fetcher_fn(nu::ShardedQueue<Item, std::true_type> remote_queue,
                  std::size_t id) {
    while (true) {
      auto status = load_acquire(&status_);

      {
        rt::ScopedLock lock(&spin_);
        if (local_queue_.size() >= kMaxLocalQueueDepth) {
          continue;
        }
      }

      auto popped = remote_queue.try_pop(kBatchSize);
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

  void gpu_fn(std::size_t id,
              nu::ShardedQueue<Item, std::true_type> remote_queue) {
    auto &traces = all_traces_[id];

    auto num_traces = 0;
    bool paused = false;

    while (true) {
      if (id >= num_gpus_) {
        if (!paused) {
          paused = true;
        }
        continue;
      }
      if (paused) {
        paused = false;
      }

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

        traces[num_traces++] = std::make_pair(start_us, end_us);
      } else {
        if (unlikely(status == GPUStatus::kDrain)) {
          break;
        }
      }
    }

    num_traces_[id] = num_traces;
  }

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> drain_and_stop() {
    status_ = GPUStatus::kDrain;
    barrier();

    for (auto &fetcher : fetchers_ths_) {
      fetcher.Join();
    }
    for (auto &gpu : gpu_ths_) {
      gpu.Join();
    }

    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> all_traces;
    for (auto [traces, size] : std::views::zip(all_traces_, num_traces_)) {
      all_traces.emplace_back(
          std::vector(traces.begin(), traces.begin() + size));
    }
    return all_traces;
  }

  void set_num_gpus(std::size_t num_gpus) { num_gpus_ = num_gpus; }

  static void process(Item &&item) { nu::Time::sleep(kProcessDelayUs); }

 private:
  GPUStatusType status_ = GPUStatus::kRunning;
  std::queue<Item> local_queue_;
  std::vector<std::array<std::pair<uint64_t, uint64_t>, kMaxNumImages>>
      all_traces_;
  std::vector<std::size_t> num_traces_;
  rt::Spin spin_;
  std::vector<rt::Thread> fetchers_ths_;
  std::vector<rt::Thread> gpu_ths_;
  std::atomic<std::size_t> num_gpus_;
};

}  // namespace imagenet

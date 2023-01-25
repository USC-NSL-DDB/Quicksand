#pragma once

#include "nu/utils/time.hpp"

namespace imagenet {

enum GPUStatus {
  kRunning = 0,
  kDrain,
  kTerminate,
};

using GPUStatusType = uint8_t;

template <typename Item>
class MockGPU {
 public:
  static constexpr uint32_t kProcessDelayUs = 3500;

  MockGPU() {}
  void run(nu::ShardedQueue<Item, std::true_type> queue) {
    constexpr std::size_t kPopNumItems = 16;

    while (true) {
      auto status = load_acquire(&status_);
      if (unlikely(status == GPUStatus::kTerminate)) {
        break;
      }
      auto popped = queue.try_pop(kPopNumItems);
      if (unlikely(status == GPUStatus::kDrain && popped.size() == 0)) {
        break;
      }
      for (auto &p : popped) {
        process(std::move(p));
      }
    }
  }
  void drain_and_stop() { status_ = GPUStatus::kDrain; }

 private:
  void process(Item &&item) { nu::Time::delay(kProcessDelayUs); }

  GPUStatusType status_ = GPUStatus::kRunning;
};
}  // namespace imagenet

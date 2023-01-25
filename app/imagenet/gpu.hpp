#pragma once

#include <sync.h>

#include <nu/utils/time.hpp>

namespace imagenet {

enum GPUStatus { kRunning = 0, kDrain, kPause };

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
      if (unlikely(status == GPUStatus::kPause)) {
        rt::Preempt p;
        rt::PreemptGuard g(&p);
        g.Park(&waker_);
      }

      auto popped = queue.try_pop(kPopNumItems);
      if (unlikely(status == GPUStatus::kDrain && popped.empty())) {
        break;
      }

      for (auto &p : popped) {
        process(std::move(p));
      }
    }
  }
  void drain_and_stop() { rt::access_once(status_) = GPUStatus::kDrain; }
  void pause() { rt::access_once(status_) = GPUStatus::kPause; }
  void resume() {
    status_ = GPUStatus::kRunning;
    barrier();
    waker_.Wake();
  }

 private:
  void process(Item &&item) { nu::Time::delay(kProcessDelayUs); }

  rt::ThreadWaker waker_;
  GPUStatusType status_ = GPUStatus::kRunning;
};
}  // namespace imagenet

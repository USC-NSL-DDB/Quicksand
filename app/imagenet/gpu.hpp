#pragma once

namespace imagenet {
template <uint32_t Micros>
void compute_us() {
  const auto num_iters = Micros * cycles_per_us;
  for (uint32_t i = 0; i < num_iters; i++) {
    asm volatile("nop");
  }
}

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
    constexpr std::size_t kPopNumItems = 10;

    while (true) {
      auto status = rt::access_once(status_);
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
  void stop() { status_ = GPUStatus::kTerminate; }

 private:
  void process(Item &&item) { compute_us<kProcessDelayUs>(); }

  GPUStatusType status_ = GPUStatus::kRunning;
};
}  // namespace imagenet

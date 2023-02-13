#include <iostream>
#include <vector>

#include "nu/commons.hpp"
#include "nu/cont_ds_range.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_queue.hpp"
#include "nu/sharded_vector.hpp"

constexpr std::size_t kImageSize = 224 * 224 * 3;
constexpr std::size_t kNumImages = 100'000;
constexpr std::size_t kNumConsumers = 2;
constexpr std::size_t kBatchSize = 16;
constexpr uint64_t kProducerPerElemWork = 2'800;
constexpr uint64_t kConsumerPerElemWork = 333;
constexpr uint64_t kScalingInterval = nu::kOneSecond;

class MockImage {
 public:
  MockImage() {}
  MockImage(std::size_t size) {
    bytes_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
      bytes_.push_back(static_cast<char>(i));
    }
  }

  std::size_t size() const { return bytes_.size(); }

  template <class Archive>
  void save(Archive &ar) const {
    ar(bytes_);
  }

  template <class Archive>
  void load(Archive &ar) {
    ar(bytes_);
  }

 private:
  std::vector<char> bytes_;
};

template <typename Q>
class StaticConsumer {
 public:
  StaticConsumer(Q queue) : queue_(std::move(queue)) {}

  void consume() {
    while (true) {
      auto batch = queue_.try_pop(kBatchSize);
      if (batch.empty() && load_acquire(&stopped_)) {
        break;
      }
      for (std::size_t i = 0; i < batch.size(); ++i) {
        nu::Time::delay(kConsumerPerElemWork);
      }
    }
  }

  void stop() { stopped_ = true; }

 private:
  Q queue_;
  bool stopped_ = false;
};

template <typename Q>
class ConsumerGroup {
 public:
  ConsumerGroup(Q queue, std::size_t max_consumers)
      : num_active_(max_consumers) {
    for (std::size_t i = 0; i < max_consumers; ++i) {
      consumers_.push_back(
          nu::async([this, queue, id = i]() { consume(queue, id); }));
    }
  }

  void consume(Q queue, std::size_t id) {
    while (true) {
      if (id >= load_acquire(&num_active_)) {
        if (load_acquire(&stopped_)) {
          break;
        } else {
          continue;
        }
      }
      auto batch = queue.try_pop(kBatchSize);
      if (batch.empty() && load_acquire(&stopped_)) {
        break;
      }
      for (std::size_t i = 0; i < batch.size(); ++i) {
        nu::Time::delay(kConsumerPerElemWork);
      }
    }
  }

  void stop() {
    stopped_ = true;
    barrier();

    for (auto &f : consumers_) {
      f.get();
    }
  }

  void set_num_consumers(std::size_t num_consumers) {
    num_active_ = num_consumers;
    barrier();
  }

 private:
  std::vector<nu::Future<void>> consumers_;
  bool stopped_ = false;
  std::size_t num_active_;
};

struct Bench {
 public:
  void bench_large_elem_producer_auto_scaling() {
    std::cout << "\t" << __FUNCTION__ << std::endl;

    auto img = MockImage(kImageSize);
    auto images = nu::make_sharded_vector<MockImage, std::false_type>();
    for (std::size_t i = 0; i < kNumImages; ++i) {
      images.push_back(img);
    }
    auto sealed_imgs = nu::to_sealed_ds(std::move(images));
    auto images_range = nu::make_contiguous_ds_range(sealed_imgs);

    auto queue = nu::make_sharded_queue<MockImage, std::true_type>();
    using Consumer = ConsumerGroup<decltype(queue)>;

    auto stopped = false;

    auto consumers = nu::async([&]() {
      auto cs = nu::make_proclet<Consumer>(
          std::forward_as_tuple(queue, kNumConsumers));

      while (true) {
        if (load_acquire(&stopped)) {
          break;
        }
        cs.run(&Consumer::set_num_consumers, kNumConsumers / 2);
        nu::Time::sleep(kScalingInterval);

        if (load_acquire(&stopped)) {
          break;
        }
        cs.run(&Consumer::set_num_consumers, kNumConsumers);
        nu::Time::sleep(kScalingInterval);
      }

      cs.run(&Consumer::stop);
    });

    barrier();
    auto t0 = microtime();
    barrier();

    auto producers = nu::make_distributed_executor(
        +[](decltype(images_range) &imgs, decltype(queue) queue) {
          while (true) {
            auto img = imgs.pop();
            if (!img) {
              break;
            }
            nu::Time::delay(kProducerPerElemWork);
            queue.push(*img);
          }
        },
        images_range, queue);

    producers.get();
    stopped = true;
    barrier();
    consumers.get();

    barrier();
    auto t1 = microtime();
    barrier();

    std::cout << "\t\tShardedQueue: " << t1 - t0 << " us" << std::endl;
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    Bench b;
    b.bench_large_elem_producer_auto_scaling();
  });
}

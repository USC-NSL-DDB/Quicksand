#include <iostream>
#include <vector>

#include "nu/cont_ds_range.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_queue.hpp"
#include "nu/sharded_vector.hpp"

constexpr std::size_t kImageSize = 224 * 224 * 3;
constexpr std::size_t kNumImages = 300'000;
constexpr std::size_t kNumConsumers = 4;
constexpr uint64_t kProducerPerElemWork = 6'250;
constexpr uint64_t kConsumerPerElemWork = 333;

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
  constexpr static std::size_t kBatchSize = 16;

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
    using Consumer = StaticConsumer<decltype(queue)>;

    auto consumers = std::vector<nu::Proclet<Consumer>>{};
    auto consume_futures = std::vector<nu::Future<void>>{};
    for (std::size_t i = 0; i < kNumConsumers; ++i) {
      consumers.emplace_back(
          nu::make_proclet<Consumer>(std::forward_as_tuple(queue)));
      consume_futures.emplace_back(
          consumers.back().run_async(&Consumer::consume));
    }

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
            nu::Time::delay(kConsumerPerElemWork);
            queue.push(*img);
          }
        },
        images_range, queue);

    producers.get();

    auto stop_futures = std::vector<nu::Future<void>>{};
    for (auto &c : consumers) {
      stop_futures.emplace_back(c.run_async(&Consumer::stop));
    }
    for (auto [stop_ft, consume_ft] :
         std::views::zip(stop_futures, consume_futures)) {
      stop_ft.get();
      consume_ft.get();
    }

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

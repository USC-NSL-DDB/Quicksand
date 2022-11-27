#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_queue.hpp"

constexpr std::size_t kElemSz = 100'000;
constexpr std::size_t kBatchSz = 1'000'000;
constexpr std::size_t kNumBatches = 1 << 14;
constexpr std::size_t kNumProducers = 4;
constexpr std::size_t kNumConsumers = 2;
constexpr uint64_t kConsumerPerElemWork = 500;

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

template <typename T>
class Batch {
 public:
  Batch() {}
  Batch(std::size_t size_threshold)
      : max_size_(size_threshold), curr_size_(0) {}

  bool add(T &&elem) {
    auto sz = elem.size();
    if (unlikely(curr_size_ + sz > max_size_)) {
      return false;
    }
    batch_.emplace_back(elem);
    curr_size_ += sz;
    return true;
  }

  template <class Archive>
  void save(Archive &ar) const {
    ar(batch_);
  }

  template <class Archive>
  void load(Archive &ar) {
    ar(batch_);
  }

 private:
  std::vector<T> batch_;
  std::size_t max_size_;
  std::size_t curr_size_;
};

class Producer {
 public:
  using BatchType = Batch<MockImage>;

  Producer(nu::ShardedQueue<Batch<MockImage>, std::true_type> queue)
      : queue_(std::move(queue)) {}

  void produce(std::size_t n_batches, std::size_t batch_sz) {
    for (std::size_t i = 0; i < n_batches; ++i) {
      queue_.push(make_batch(batch_sz));
    }
  }

 private:
  nu::ShardedQueue<Batch<MockImage>, std::true_type> queue_;

  BatchType make_batch(std::size_t batch_sz) {
    auto batch = Batch<MockImage>(batch_sz);
    while (batch.add(MockImage(kElemSz)))
      ;
    return batch;
  }
};

class Consumer {
 public:
  using BatchType = Batch<MockImage>;

  Consumer(nu::ShardedQueue<Batch<MockImage>, std::true_type> queue,
           uint64_t work)
      : queue_(std::move(queue)), work_(work) {}

  void consume(std::size_t n_batches) {
    for (std::size_t i = 0; i < n_batches; ++i) {
      auto batch = queue_.dequeue();
      do_work(std::move(batch));
    }
  }

 private:
  nu::ShardedQueue<Batch<MockImage>, std::true_type> queue_;
  uint64_t work_;

  void do_work(Batch<MockImage> batch) {
    auto target = microtime() + work_;
    while (microtime() < target)
      ;
  }
};

struct Bench {
 public:
  void bench_batched_queue_produce() {
    std::cout << "\tbench_batched_queue_produce()" << std::endl;

    std::size_t elems_per_producer = kNumBatches / kNumProducers;

    auto queue = nu::make_sharded_queue<Batch<MockImage>, std::true_type>();

    std::vector<nu::Proclet<Producer>> producers;
    for (std::size_t i = 0; i < kNumProducers; ++i) {
      producers.emplace_back(nu::make_proclet<Producer>(queue));
    }

    std::vector<nu::Future<void>> futures;
    for (std::size_t i = 0; i < kNumProducers; ++i) {
      futures.emplace_back(producers[i].run_async(
          &Producer::produce, elems_per_producer, kBatchSz));
    }

    auto t0 = microtime();
    for (auto &future : futures) {
      future.get();
    }
    auto t1 = microtime();

    auto ops = static_cast<double>(kNumBatches * 1'000'000) / (t1 - t0);
    auto bw = (ops / 1'000'000) * kBatchSz;
    std::cout << "\t\tShardedQueue: " << t1 - t0 << " us, " << ops << " OPS, "
              << bw << " MB/s" << std::endl;
  }

  void bench_batched_queue_produce_consume() {
    std::cout << "\tbench_batched_queue_produce_consume()" << std::endl;

    std::size_t elems_per_producer = kNumBatches / kNumProducers;
    std::size_t elems_per_consumer = kNumBatches / kNumConsumers;
    std::size_t avg_batch_sz = kBatchSz / kElemSz;
    uint64_t work_per_batch = kConsumerPerElemWork * avg_batch_sz;

    auto queue = nu::make_sharded_queue<Batch<MockImage>, std::true_type>();

    std::vector<nu::Proclet<Producer>> producers;
    for (std::size_t i = 0; i < kNumProducers; ++i) {
      producers.emplace_back(nu::make_proclet<Producer>(queue));
    }
    std::vector<nu::Proclet<Consumer>> consumers;
    for (std::size_t i = 0; i < kNumConsumers; ++i) {
      consumers.emplace_back(nu::make_proclet<Consumer>(queue, work_per_batch));
    }

    std::vector<nu::Future<void>> futures;
    for (std::size_t i = 0; i < kNumProducers; ++i) {
      futures.emplace_back(producers[i].run_async(
          &Producer::produce, elems_per_producer, kBatchSz));
    }
    for (std::size_t i = 0; i < kNumConsumers; ++i) {
      futures.emplace_back(
          consumers[i].run_async(&Consumer::consume, elems_per_consumer));
    }

    auto t0 = microtime();
    for (auto &future : futures) {
      future.get();
    }
    auto t1 = microtime();

    auto ops = static_cast<double>(kNumBatches * 1'000'000) / (t1 - t0);
    auto bw = (ops / 1'000'000) * kBatchSz;
    std::cout << "\t\tShardedQueue: " << t1 - t0 << " us, " << ops << " OPS, "
              << bw << " MB/s" << std::endl;
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    Bench b;
    // b.bench_batched_queue_produce();
    b.bench_batched_queue_produce_consume();
  });
}

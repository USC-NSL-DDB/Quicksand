#include <iostream>
#include <string>
#include <type_traits>

#include "nu/dis_executor.hpp"
#include "nu/queue_range.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_queue.hpp"
#include "nu/utils/time.hpp"

using namespace nu;

std::vector<char> make_byte_vec(std::size_t size) {
  std::vector<char> bytes;
  bytes.reserve(size);

  for (std::size_t i = 0; i < size; ++i) {
    bytes.push_back(static_cast<char>(i));
  }

  return bytes;
}

std::vector<std::vector<char>> make_batch(std::size_t size,
                                          std::size_t elem_sz) {
  assert(size >= elem_sz);
  std::size_t batch_sz = size / elem_sz;

  std::vector<std::vector<char>> batch;
  batch.reserve(batch_sz);
  for (std::size_t i = 0; i < batch_sz; ++i) {
    batch.emplace_back(make_byte_vec(elem_sz));
  }

  return batch;
}

template <typename T>
struct Producer {
  Producer() = default;
  void produce(std::size_t n_elems, T elem,
               nu::ShardedQueue<T, std::true_type> queue) {
    for (std::size_t i = 0; i < n_elems; ++i) {
      queue.push(elem);
    }
  }
};

template <typename T>
struct BlockingConsumer {
  BlockingConsumer() = default;
  std::vector<T> consume(std::size_t n_elems,
                         nu::ShardedQueue<T, std::true_type> queue) {
    std::vector<T> elems;

    for (std::size_t i = 0; i < n_elems; ++i) {
      elems.emplace_back(queue.pop());
    }
    return elems;
  }
};

template <typename T>
struct NonBlockingConsumer {
  constexpr static std::size_t kBatchSize = 2;

  NonBlockingConsumer() = default;
  std::vector<T> consume(std::size_t n_elems,
                         nu::ShardedQueue<T, std::true_type> queue) {
    std::vector<T> elems;

    while (n_elems) {
    retry:
      auto popped = queue.try_pop(std::min(kBatchSize, n_elems));
      if (unlikely(popped.empty())) {
        Time::sleep(200);
        goto retry;
      }
      for (auto &d : popped) {
        elems.emplace_back(std::move(d));
      }
      n_elems -= popped.size();
    }
    return elems;
  }
};

bool test_push_and_pop() {
  constexpr uint32_t k_size = 24 << 12;
  auto queue = make_sharded_queue<int, std::true_type>();

  int front = 0;
  for (uint32_t i = 0; i < k_size; i++) {
    queue.push(i);
  }
  if (queue.front() != front) {
    return false;
  }
  if (queue.back() != k_size - 1) {
    return false;
  }

  front += 1 << 10;
  for (uint32_t i = 0; i < (1 << 10); i++) {
    queue.pop();
  }
  if (queue.front() != front) {
    return false;
  }

  return true;
}

bool test_size_and_empty() {
  constexpr uint32_t k_size = 24 << 12;
  auto queue = make_sharded_queue<int, std::true_type>();

  if (!queue.empty()) {
    return false;
  }

  for (uint32_t i = 0; i < k_size; i++) {
    queue.push(i);
  }
  if (queue.size() != k_size) {
    return false;
  }

  queue.pop();
  if (queue.size() != k_size - 1) {
    return false;
  }
  if (queue.empty()) {
    return false;
  }

  return true;
}

bool test_batched_queue() {
  constexpr std::size_t queue_sz = 1 << 12;
  constexpr std::size_t batch_sz = 1'000'000;
  constexpr std::size_t kBatchElemSz = 100'000;

  auto queue =
      make_sharded_queue<std::vector<std::vector<char>>, std::true_type>();

  for (std::size_t i = 0; i < queue_sz; ++i) {
    queue.push(make_batch(batch_sz, kBatchElemSz));
  }

  auto expected = make_batch(batch_sz, kBatchElemSz);
  for (std::size_t i = 0; i < queue_sz; ++i) {
    if (queue.empty()) {
      return false;
    }
    auto dequeued = queue.pop();
    if (dequeued != expected) {
      return false;
    }
  }

  return true;
}

template <bool Blocking>
bool test_dequeue() {
  using Consumer = std::conditional_t<Blocking, BlockingConsumer<int>,
                                      NonBlockingConsumer<int>>;
  constexpr std::size_t kNumElems = 1 << 20;
  constexpr int kElem = 33;
  constexpr std::size_t kProducerCount = 4;
  constexpr std::size_t kConsumerCount = 32;

  auto queue = make_sharded_queue<int, std::true_type>();
  auto producers = std::vector<Proclet<Producer<int>>>{};
  auto consumers = std::vector<Proclet<Consumer>>{};

  for (std::size_t i = 0; i < kProducerCount; ++i) {
    producers.emplace_back(make_proclet<Producer<int>>());
  }
  for (std::size_t i = 0; i < kConsumerCount; ++i) {
    consumers.emplace_back(make_proclet<Consumer>());
  }

  auto consumer_futures = std::vector<nu::Future<std::vector<int>>>{};
  for (auto &consumer : consumers) {
    consumer_futures.emplace_back(consumer.run_async(
        &Consumer::consume, kNumElems / kConsumerCount, queue));
  }

  auto producer_futures = std::vector<nu::Future<void>>{};
  for (auto &producer : producers) {
    producer_futures.emplace_back(producer.run_async(
        &Producer<int>::produce, kNumElems / kProducerCount, kElem, queue));
  }

  for (auto &f : producer_futures) {
    f.get();
  }

  auto dequeued = std::vector<int>{};
  for (auto &f : consumer_futures) {
    auto elems = f.get();
    dequeued.insert(dequeued.end(), std::make_move_iterator(elems.begin()),
                    std::make_move_iterator(elems.end()));
  }

  if (dequeued.size() != kNumElems) {
    return false;
  }

  return std::all_of(dequeued.cbegin(), dequeued.cend(),
                     [](int x) { return x == kElem; });
}

bool test_blocking_enqueue() {
  constexpr std::size_t kNumElems = 1 << 12;
  constexpr std::size_t kBatchBytes = 100'000;
  constexpr std::size_t kBatchElemSz = 1'000;
  constexpr std::size_t kQueueMaxSizeBytes = 1 << 26;
  constexpr std::size_t kProducerCount = 16;
  constexpr std::size_t kConsumerCount = 8;

  using Elem = std::vector<std::vector<char>>;

  Elem elem = make_batch(kBatchBytes, kBatchElemSz);

  auto queue = make_sharded_queue<Elem, std::true_type>(kQueueMaxSizeBytes);
  auto producers = std::vector<Proclet<Producer<Elem>>>{};
  auto consumers = std::vector<Proclet<BlockingConsumer<Elem>>>{};

  for (std::size_t i = 0; i < kProducerCount; ++i) {
    producers.emplace_back(make_proclet<Producer<Elem>>());
  }
  for (std::size_t i = 0; i < kConsumerCount; ++i) {
    consumers.emplace_back(make_proclet<BlockingConsumer<Elem>>());
  }

  auto producer_futures = std::vector<nu::Future<void>>{};
  for (auto &producer : producers) {
    producer_futures.emplace_back(producer.run_async(
        &Producer<Elem>::produce, kNumElems / kProducerCount, elem, queue));
  }

  auto consumer_futures = std::vector<nu::Future<std::vector<Elem>>>{};
  for (auto &consumer : consumers) {
    consumer_futures.emplace_back(consumer.run_async(
        &BlockingConsumer<Elem>::consume, kNumElems / kConsumerCount, queue));
  }

  for (auto &f : producer_futures) {
    f.get();
  }

  auto dequeued = std::vector<Elem>{};
  for (auto &f : consumer_futures) {
    auto elems = f.get();
    dequeued.insert(dequeued.end(), std::make_move_iterator(elems.begin()),
                    std::make_move_iterator(elems.end()));
  }

  if (dequeued.size() != kNumElems) {
    return false;
  }

  return std::all_of(dequeued.cbegin(), dequeued.cend(),
                     [&](auto x) { return x == elem; });
}

bool test_rate_matching() {
  // TODO: check correctness

  auto queue = nu::make_sharded_queue<int, std::true_type>();

  auto produce_rng = nu::make_writeable_queue_range(queue);
  auto consume_rng = nu::make_queue_range(queue);

  auto producers = nu::make_distributed_executor(
      +[](decltype(produce_rng) &rng) {
        while (true) {
          auto inserter = rng.pop();
          inserter = 33;
        }
      },
      produce_rng);

  auto consumers = nu::make_distributed_executor(
      +[](decltype(consume_rng) &rng) {
        while (true) {
          rng.pop();
        }
      },
      consume_rng);

  return true;
}

bool run_test() {
  return test_push_and_pop() && test_size_and_empty() && test_batched_queue() &&
         test_dequeue</* Blocking = */ false>() &&
         test_dequeue</* Blocking = */ true>() && test_blocking_enqueue();
}

void do_work() {
  if (run_test()) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

#include <iostream>
#include <string>
#include <type_traits>

#include "nu/dis_executor.hpp"
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

template <typename Q>
struct Producer {
  Producer() = default;
  void produce(std::size_t n_elems, typename Q::Val elem, Q queue) {
    for (std::size_t i = 0; i < n_elems; ++i) {
      queue.push(elem);
    }
  }
};

template <typename Q>
struct BlockingConsumer {
  BlockingConsumer() = default;
  std::vector<typename Q::Val> consume(std::size_t n_elems, Q queue) {
    std::vector<typename Q::Val> elems;
    for (std::size_t i = 0; i < n_elems; ++i) {
      elems.emplace_back(queue.pop());
    }
    return elems;
  }
};

template <typename Q>
struct NonBlockingConsumer {
  constexpr static std::size_t kBatchSize = 2;

  NonBlockingConsumer() = default;
  std::vector<typename Q::Val> consume(std::size_t n_elems, Q queue) {
    std::vector<typename Q::Val> elems;

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

template <BoolIntegral Blocking, BoolIntegral LL>
bool test_enqueue_dequeue() {
  using Elem = int;
  using Queue = nu::ShardedQueue<Elem, LL>;
  using Producer = Producer<Queue>;
  using Consumer = std::conditional_t<Blocking::value, BlockingConsumer<Queue>,
                                      NonBlockingConsumer<Queue>>;
  constexpr std::size_t kNumElems = 1 << 20;
  constexpr int kElem = 33;
  constexpr std::size_t kProducerCount = 4;
  constexpr std::size_t kConsumerCount = 32;

  Queue queue = make_sharded_queue<Elem, LL>();
  auto producers = std::vector<Proclet<Producer>>{};
  auto consumers = std::vector<Proclet<Consumer>>{};

  for (std::size_t i = 0; i < kProducerCount; ++i) {
    producers.emplace_back(make_proclet<Producer>());
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
        &Producer::produce, kNumElems / kProducerCount, kElem, queue));
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

bool test_enqueue_dequeue() {
  return test_enqueue_dequeue</* Blocking = */ std::false_type,
                              /* LL = */ std::false_type>() &&
         test_enqueue_dequeue</* Blocking = */ std::true_type,
                              /* LL = */ std::false_type>() &&
         test_enqueue_dequeue</* Blocking = */ std::false_type,
                              /* LL = */ std::true_type>() &&
         test_enqueue_dequeue</* Blocking = */ std::true_type,
                              /* LL = */ std::true_type>();
}

bool run_test() {
  return test_push_and_pop() && test_size_and_empty() && test_batched_queue() &&
         test_enqueue_dequeue();
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

#include <iostream>
#include <string>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_queue.hpp"

using namespace nu;

bool test_push_and_pop() {
  // TODO: increase test size when LL=false is implemented
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

  // FIXME: This may fail if merge is not implemented
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

std::vector<char> make_byte_vec(std::size_t size) {
  std::vector<char> bytes;
  bytes.reserve(size);

  for (std::size_t i = 0; i < size; ++i) {
    bytes.push_back(static_cast<char>(i));
  }

  return bytes;
}

std::vector<std::vector<char>> make_batch(std::size_t size) {
  std::size_t elem_sz = 100'000;

  assert(size > elem_sz);
  std::size_t batch_sz = size / elem_sz;

  std::vector<std::vector<char>> batch;
  batch.reserve(batch_sz);
  for (std::size_t i = 0; i < batch_sz; ++i) {
    batch.emplace_back(make_byte_vec(elem_sz));
  }

  return batch;
}

bool test_batched_queue() {
  constexpr std::size_t queue_sz = 1 << 10;
  constexpr std::size_t batch_sz = 1'000'000;

  auto queue =
      make_sharded_queue<std::vector<std::vector<char>>, std::true_type>();

  for (std::size_t i = 0; i < queue_sz; ++i) {
    queue.push(make_batch(batch_sz));
  }

  auto expected = make_batch(batch_sz);
  for (std::size_t i = 0; i < queue_sz; ++i) {
    if (queue.empty()) {
      return false;
    }
    auto dequeued = queue.dequeue();
    if (dequeued != expected) {
      return false;
    }
  }

  return true;
}

template <typename T>
struct Producer {
  Producer() {}
  void produce(std::size_t n_elems, T elem,
               nu::ShardedQueue<T, std::true_type> queue) {
    for (std::size_t i = 0; i < n_elems; ++i) {
      queue.push(elem);
    }
  }
};

template <typename T>
struct Consumer {
  Consumer() {}
  std::vector<T> consume(std::size_t n_elems,
                         nu::ShardedQueue<T, std::true_type> queue) {
    std::vector<T> elems;
    for (std::size_t i = 0; i < n_elems; ++i) {
      elems.emplace_back(queue.dequeue());
    }
    return elems;
  }
};

bool test_blocking_dequeue() {
  constexpr std::size_t num_elems = 1 << 20;
  constexpr int elem = 33;
  constexpr std::size_t n_producers = 4;
  constexpr std::size_t n_consumers = 32;

  auto queue = make_sharded_queue<int, std::true_type>();
  auto producers = std::vector<Proclet<Producer<int>>>{};
  auto consumers = std::vector<Proclet<Consumer<int>>>{};

  for (std::size_t i = 0; i < n_producers; ++i) {
    producers.emplace_back(make_proclet<Producer<int>>());
  }
  for (std::size_t i = 0; i < n_consumers; ++i) {
    consumers.emplace_back(make_proclet<Consumer<int>>());
  }

  auto consumer_futures = std::vector<nu::Future<std::vector<int>>>{};
  for (auto &consumer : consumers) {
    consumer_futures.emplace_back(consumer.run_async(
        &Consumer<int>::consume, num_elems / n_consumers, queue));
  }

  auto producer_futures = std::vector<nu::Future<void>>{};
  for (auto &producer : producers) {
    producer_futures.emplace_back(producer.run_async(
        &Producer<int>::produce, num_elems / n_producers, elem, queue));
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

  if (dequeued.size() != num_elems) {
    return false;
  }

  return std::all_of(dequeued.cbegin(), dequeued.cend(),
                     [](int x) { return x == elem; });
}

bool run_test() {
  auto passed = test_push_and_pop() && test_size_and_empty() &&
                test_batched_queue() && test_blocking_dequeue();

  return passed;
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

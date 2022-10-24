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

  // This may fail if merge is not implemented
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

bool run_test() {
  if (!test_push_and_pop()) {
    return false;
  }
  if (!test_size_and_empty()) {
    return false;
  }

  return true;
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

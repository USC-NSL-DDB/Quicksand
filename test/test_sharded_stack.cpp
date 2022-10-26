#include <iostream>
#include <string>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_stack.hpp"

using namespace nu;

bool test_push_and_pop() {
  // TODO: increase test size when LL=false is implemented
  constexpr uint32_t k_size = 24 << 12;
  auto stack = make_sharded_stack<int, std::true_type>();

  int top = k_size - 1;
  for (uint32_t i = 0; i < k_size; i++) {
    stack.push(i);
  }
  if (stack.top() != top) {
    return false;
  }

  // FIXME: This may fail if merge is not implemented
  top -= 1 << 10;
  for (uint32_t i = 0; i < (1 << 10); i++) {
    stack.pop();
  }
  if (stack.top() != top) {
    return false;
  }

  return true;
}

bool test_size_and_empty() {
  constexpr uint32_t k_size = 24 << 12;
  auto stack = make_sharded_stack<int, std::true_type>();

  if (!stack.empty()) {
    return false;
  }

  for (uint32_t i = 0; i < k_size; i++) {
    stack.push(i);
  }
  if (stack.size() != k_size) {
    return false;
  }

  stack.pop();
  if (stack.size() != k_size - 1) {
    return false;
  }
  if (stack.empty()) {
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

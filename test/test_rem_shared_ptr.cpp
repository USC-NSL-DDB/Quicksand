#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/rem_obj.hpp"
#include "nu/rem_shared_ptr.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class Obj {};

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  auto rem_obj = RemObj<Obj>::create();

  auto rem_shared_ptr_a_future = rem_obj.run_async(
      +[](Obj &_, std::vector<int> vec_a) {
        auto shared_ptr_a =
            std::make_shared<std::vector<int>>(std::move(vec_a));
        return RemSharedPtr(std::move(shared_ptr_a));
      },
      a);
  auto rem_shared_ptr_b_future = rem_obj.run_async(
      +[](Obj &_, std::vector<int> vec_b) {
        auto shared_ptr_b =
            std::make_shared<std::vector<int>>(std::move(vec_b));
        return RemSharedPtr(std::move(shared_ptr_b));
      },
      b);

  auto rem_shared_ptr_a = std::move(rem_shared_ptr_a_future.get());
  auto rem_shared_ptr_a_copy = rem_shared_ptr_a;
  auto rem_shared_ptr_b = std::move(rem_shared_ptr_b_future.get());
  auto rem_shared_ptr_b_copy = rem_shared_ptr_b;
  auto c = rem_obj.run(
      +[](Obj &_, RemSharedPtr<std::vector<int>> &&rem_shared_ptr_a,
          RemSharedPtr<std::vector<int>> &&rem_shared_ptr_b) {
        auto *raw_ptr_a = rem_shared_ptr_a.get_checked();
        auto *raw_ptr_b = rem_shared_ptr_b.get_checked();
        std::vector<int> rem_c;
        for (size_t i = 0; i < raw_ptr_a->size(); i++) {
          rem_c.push_back(raw_ptr_a->at(i) + raw_ptr_b->at(i));
        }
        return rem_c;
      },
      std::move(rem_shared_ptr_a), std::move(rem_shared_ptr_b));

  passed &= !rem_shared_ptr_a;
  passed &= !rem_shared_ptr_b;
  passed &= rem_shared_ptr_a_copy;
  passed &= rem_shared_ptr_b_copy;

  for (size_t i = 0; i < a.size(); i++) {
    if (c[i] != a[i] + b[i]) {
      passed = false;
      break;
    }
  }
  if (a.size() != c.size()) {
    passed = false;
  }

  auto a_copy = *rem_shared_ptr_a_copy;
  for (size_t i = 0; i < a.size(); i++) {
    if (a_copy[i] != a[i]) {
      passed = false;
      break;
    }
  }
  if (a.size() != a_copy.size()) {
    passed = false;
  }

  auto b_copy = *rem_shared_ptr_b_copy;
  for (size_t i = 0; i < b.size(); i++) {
    if (b_copy[i] != b[i]) {
      passed = false;
      break;
    }
  }
  if (b.size() != b_copy.size()) {
    passed = false;
  }

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

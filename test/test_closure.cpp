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

#include "rem_obj.hpp"
#include "rem_ptr.hpp"
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class Obj {};

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  // Intentionally test the async method.
  auto rem_obj_future = RemObj<Obj>::create_async();
  auto rem_obj = std::move(rem_obj_future.get());

  auto rem_ptr_a_future = rem_obj.run_async(
      +[](Obj &_, std::vector<int> vec_a) {
        auto *raw_ptr_a = new std::vector<int>();
        *raw_ptr_a = vec_a;
        return to_rem_ptr(raw_ptr_a);
      },
      a);
  auto rem_ptr_b_future = rem_obj.run_async(
      +[](Obj &_, std::vector<int> vec_b) {
        auto *raw_ptr_b = new std::vector<int>();
        *raw_ptr_b = vec_b;
        return to_rem_ptr(raw_ptr_b);
      },
      b);

  auto rem_ptr_a = rem_ptr_a_future.get();
  auto rem_ptr_b = rem_ptr_b_future.get();
  auto c = rem_obj.run(
      +[](Obj &_, RemPtr<std::vector<int>> rem_ptr_a,
          RemPtr<std::vector<int>> rem_ptr_b) {
        auto *raw_ptr_a = rem_ptr_a.get_checked();
        auto *raw_ptr_b = rem_ptr_b.get_checked();
        std::vector<int> rem_c;
        for (size_t i = 0; i < raw_ptr_a->size(); i++) {
          rem_c.push_back(raw_ptr_a->at(i) + raw_ptr_b->at(i));
        }
        return rem_c;
      },
      rem_ptr_a, rem_ptr_b);

  for (size_t i = 0; i < a.size(); i++) {
    if (c[i] != a[i] + b[i]) {
      passed = false;
      break;
    }
  }
  if (a.size() != c.size()) {
    passed = false;
  }

  auto a_copy = *rem_ptr_a;
  for (size_t i = 0; i < a.size(); i++) {
    if (a_copy[i] != a[i]) {
      passed = false;
      break;
    }
  }
  if (a.size() != a_copy.size()) {
    passed = false;
  }

  auto b_copy = *rem_ptr_b;
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
  int ret;
  std::string mode_str;

  if (argc < 3) {
    goto wrong_args;
  }

  mode_str = std::string(argv[2]);
  if (mode_str == "CLT") {
    mode = Runtime::Mode::CLIENT;
  } else if (mode_str == "SRV") {
    mode = Runtime::Mode::SERVER;
  } else if (mode_str == "CTL") {
    mode = Runtime::Mode::CONTROLLER;
  } else {
    goto wrong_args;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
    std::cout << "Running " << __FILE__ "..." << std::endl;

    netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
    auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                                 /* local_migrator_port = */ 8002,
                                 /* remote_ctrl_addr = */ remote_ctrl_addr,
                                 /* mode = */ mode);
    do_work();
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL" << std::endl;
  return -EINVAL;
}

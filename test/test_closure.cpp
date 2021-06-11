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
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class Obj {};

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  auto rem_obj = RemObj<Obj>::create();
  auto rem_a_ptr_future = rem_obj.run_async(
      +[](Obj &_, std::vector<int> a_vec) {
        auto *rem_a_ptr = new std::vector<int>();
        *rem_a_ptr = a_vec;
        return rem_a_ptr;
      },
      a);
  auto rem_b_ptr_future = rem_obj.run_async(
      +[](Obj &_, std::vector<int> b_vec) {
        auto *rem_b_ptr = new std::vector<int>();
        *rem_b_ptr = b_vec;
        return rem_b_ptr;
      },
      b);

  // Cannot be implemented for now.

  // auto rem_a_ptr = rem_a_ptr_future.get();
  // auto rem_b_ptr = rem_b_ptr_future.get();
  // auto c = rem_obj.run(
  //     +[](Obj &_, std::vector<int> *rem_a_ptr, std::vector<int> *rem_b_ptr) {
  //       std::vector<int> rem_c;
  //       for (size_t i = 0; i < rem_a_ptr->size(); i++) {
  //         rem_c.push_back(rem_a_ptr->at(i) + rem_b_ptr->at(i));
  //       }
  //       return rem_c;
  //     },
  //     rem_a_ptr, rem_b_ptr);

  // for (size_t i = 0; i < a.size(); i++) {
  //   if (c[i] != a[i] + b[i]) {
  //     passed = false;
  //     break;
  //   }
  // }

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

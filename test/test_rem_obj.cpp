#include <algorithm>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <utility>
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

class Obj {
public:
  void set_vec_a(std::vector<int> vec) { a_ = vec; }
  void set_vec_b(std::vector<int> vec) { b_ = vec; }
  std::vector<int> plus() {
    std::vector<int> c;
    for (size_t i = 0; i < a_.size(); i++) {
      c.push_back(a_[i] + b_[i]);
    }
    return c;
  }

private:
  std::vector<int> a_;
  std::vector<int> b_;
};

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  // Intentionally test the async method.
  auto rem_obj_future = RemObj<Obj>::create_async();
  auto rem_obj = std::move(rem_obj_future.get());

  auto future_0 = rem_obj.run_async(&Obj::set_vec_a, a);
  auto future_1 = rem_obj.run_async(&Obj::set_vec_b, b);
  future_0.get();
  future_1.get();

  auto tmp_obj = RemObj<ErasedType>::create();
  bool match;
  // We can move a RemObj into/out of closure without updating the ref cnt.
  std::tie(rem_obj, match) = tmp_obj.run(
      +[](ErasedType &, RemObj<Obj> &&rem_obj, std::vector<int> &&a,
          std::vector<int> &&b) {
        auto c = rem_obj.run(&Obj::plus);
        for (size_t i = 0; i < a.size(); i++) {
          if (c[i] != a[i] + b[i]) {
            return std::make_pair(std::move(rem_obj), false);
          }
        }
        return std::make_pair(std::move(rem_obj), true);
      },
      std::move(rem_obj), std::move(a), std::move(b));
  passed &= match;

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

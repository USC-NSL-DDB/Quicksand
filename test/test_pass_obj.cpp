#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}

#include "rem_obj.hpp"
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class VecStore {
public:
  VecStore(const std::vector<int> &a, const std::vector<int> &b)
      : a_(a), b_(b) {}
  std::vector<int> get_vec_a() { return a_; }
  std::vector<int> get_vec_b() { return b_; }

private:
  std::vector<int> a_;
  std::vector<int> b_;
};

class Adder {
public:
  std::vector<int> add(const std::vector<int> &vec_a,
                       const std::vector<int> &vec_b) {
    std::vector<int> vec_c;
    for (size_t i = 0; i < vec_a.size(); i++) {
      vec_c.push_back(vec_a[i] + vec_b[i]);
    }
    return vec_c;
  }
};

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  auto rem_vec = RemObj<VecStore>::create(a, b);
  auto rem_adder = RemObj<Adder>::create();
  auto c = rem_adder.run(
      +[](Adder &adder, RemObj<VecStore>::Cap cap) {
        auto rem_obj = RemObj<VecStore>::attach(cap);
        auto vec_a = rem_obj.run(&VecStore::get_vec_a);
        auto vec_b = rem_obj.run(&VecStore::get_vec_b);
        return adder.add(vec_a, vec_b);
      },
      rem_vec.get_cap());

  for (size_t i = 0; i < a.size(); i++) {
    if (c[i] != a[i] + b[i]) {
      passed = false;
      break;
    }
  }

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;

  netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
  auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                               /* local_migrator_port = */ 8002,
                               /* remote_ctrl_addr = */ remote_ctrl_addr,
                               /* mode = */ mode);
  do_work();
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

  ret = runtime_init(argv[1], _main, NULL);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL" << std::endl;
  return -EINVAL;
}

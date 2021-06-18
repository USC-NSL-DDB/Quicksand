#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <cstdint>
#include <iostream>
#include <memory>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "dis_mem_pool.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"

using namespace nu;

Runtime::Mode mode;

bool run_test() {
  std::vector<int> a{1, 2, 3, 4, 5, 6};

  auto rem_obj = RemObj<ErasedType>::create();
  auto [dis_mem_pool, rem_raw_ptr] = rem_obj.run(
      +[](ErasedType &, std::vector<int> a) {
        DistributedMemPool dis_mem_pool;
        return std::make_pair(std::move(dis_mem_pool),
                              dis_mem_pool.allocate_raw<std::vector<int>>(a));
      },
      a);

  auto rem_unique_ptr = dis_mem_pool.allocate_unique<std::vector<int>>(a);
  auto rem_shared_ptr = dis_mem_pool.allocate_shared<std::vector<int>>(a);

  if (a != *rem_raw_ptr) {
    return false;
  }
  dis_mem_pool.free_raw(rem_raw_ptr);

  if (a != *rem_unique_ptr) {
    return false;
  }

  if (a != *rem_shared_ptr) {
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

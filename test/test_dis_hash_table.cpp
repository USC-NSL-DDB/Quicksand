#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "cereal/types/optional.hpp"
#include "cereal/types/string.hpp"
#include "dis_hash_table.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"
#include "utils/farmhash.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr size_t kNumPairs = 100000;
constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr auto kFarmHashStrtoU64 = [](const std::string &str) {
  return util::Hash64(str.c_str(), str.size());
};

using K = std::string;
using V = std::string;

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('A', 'z');

std::string random_str(uint32_t len) {
  std::string str = "";
  for (uint32_t i = 0; i < len; i++) {
    str += dist(mt);
  }
  return str;
}

bool run_test() {
  std::unordered_map<std::string, std::string> std_map;
  auto hash_table =
      std::make_unique<DistributedHashTable<std::string, std::string>>();
  for (uint32_t i = 0; i < kNumPairs; i++) {
    std::string k = random_str(kKeyLen);
    std::string v = random_str(kValLen);
    std_map[k] = v;
    hash_table->put(k, v);
  }

  auto attached_hash_table =
      std::make_unique<DistributedHashTable<std::string, std::string>>(
          hash_table->get_cap());

  for (auto &[k, v] : std_map) {
    auto optional = attached_hash_table->get(k);
    if (!optional || v != *optional) {
      return false;
    }
  }

  auto moved_hash_table =
      std::make_unique<DistributedHashTable<std::string, std::string>>(
          std::move(*attached_hash_table));
  for (auto &[k, v] : std_map) {
    auto optional = moved_hash_table->get(k);
    if (!optional || v != *optional) {
      return false;
    }
  }

  for (auto &[k, _] : std_map) {
    if (!moved_hash_table->remove(k)) {
      return false;
    }
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

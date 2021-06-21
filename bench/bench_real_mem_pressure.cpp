#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "dis_hash_table.hpp"
#include "monitor.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"
#include "utils/farmhash.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr uint64_t kAllocateGranularity = 1ULL << 30;

struct Key {
  char data[kKeyLen];

  bool operator==(const Key &o) const {
    return __builtin_memcmp(data, o.data, kKeyLen) == 0;
  }

  template <class Archive> void serialize(Archive &ar) { ar(data); }
};

struct Val {
  char data[kValLen];

  template <class Archive> void serialize(Archive &ar) { ar(data); }
};

constexpr static auto kFarmHashKeytoU64 = [](const Key &key) {
  return util::Hash64(key.data, kKeyLen);
};

using DSHashTable = DistributedHashTable<Key, Val, decltype(kFarmHashKeytoU64)>;

void do_work() {
  DSHashTable hash_table;
  std::cout << "Now let's start the second server. Press enter to continue."
            << std::endl;
  std::cin.ignore();

  uint64_t allocated_size = 0;
  while (true) {
    mmap(nullptr, kAllocateGranularity, PROT_READ | PROT_WRITE,
         MAP_ANONYMOUS | MAP_SHARED | MAP_POPULATE, -1, 0);
    allocated_size += kAllocateGranularity;
    std::cout << "Have allocated " << allocated_size
              << " bytes. Press enter to continue." << std::endl;
    std::cin.ignore();
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

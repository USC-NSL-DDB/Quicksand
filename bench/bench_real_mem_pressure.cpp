#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <sys/sysinfo.h>
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
constexpr uint32_t kAllocateIters = 35;
constexpr uint32_t kLoggingIntervalUs = 1000;

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

struct Trace {
  uint64_t time_us;
  uint64_t free_ram;
};

void logging(const bool &done, std::vector<Trace> *traces) {
  auto last_poll_us = microtime();
  while (!ACCESS_ONCE(done)) {
    timer_sleep(last_poll_us + kLoggingIntervalUs - microtime());
    last_poll_us = microtime();

    struct sysinfo info;
    BUG_ON(sysinfo(&info) != 0);
    traces->emplace_back(microtime(), info.freeram);
  }
}

void do_work() {
  DSHashTable hash_table;
  std::cout << "Now let's start the second server. Press enter to continue."
            << std::endl;
  std::cin.ignore();

  bool done = false;
  std::vector<Trace> traces;
  auto logging_thread =
      rt::Thread([&traces, &done] { logging(done, &traces); });

  for (uint32_t i = 0; i < kAllocateIters; i++) {
    std::cout << microtime() << " " << i << std::endl;
    mmap(nullptr, kAllocateGranularity, PROT_READ | PROT_WRITE,
         MAP_ANONYMOUS | MAP_SHARED | MAP_POPULATE, -1, 0);
  }

  done = true;
  barrier();
  logging_thread.Join();

  std::ofstream ofs("log", std::ofstream::trunc);
  for (auto [time_us, free_mem] : traces) {
    ofs << time_us << " " << free_mem << std::endl;
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

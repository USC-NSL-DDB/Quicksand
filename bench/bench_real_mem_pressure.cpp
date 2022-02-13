#include <algorithm>
#include <csignal>
#include <cstdlib>
#include <vector>
#include <fstream>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <thread>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/time.h>
#include <runtime/timer.h>
}
#include <runtime.h>
#include <sync.h>
#include <thread.h>

#include "nu/commons.hpp"

constexpr uint64_t kAllocateGranularity = 1ULL << 23;
constexpr uint32_t kLoggingIntervalUs = 1000;
constexpr uint32_t kFreeMemMBTarget0 = 10000;
constexpr uint32_t kFreeMemMBTarget1 = 900;

bool signalled = false;

struct Trace {
  uint64_t time_us;
  uint64_t ram;
};

void clear_linux_cache() {
  BUG_ON(system("sync; echo 3 > /proc/sys/vm/drop_caches") != 0);
}

void logging(const bool &done, std::vector<Trace> *avail_mem_traces) {
  auto last_poll_us = microtime();
  while (!rt::access_once(done)) {
    int32_t next_poll_us = last_poll_us + kLoggingIntervalUs;
    auto headroom = next_poll_us - microtime();
    if (headroom > 0) {
      timer_sleep(headroom);
    }
    last_poll_us = microtime();

    struct sysinfo info;
    sysinfo(&info);
    avail_mem_traces->emplace_back(last_poll_us, info.freeram);
  }
}

void do_mmap_until(uint32_t free_mem_mbytes_target,
                   std::vector<Trace> *alloc_mem_traces) {
  auto mmap_times = 0;
  while (true) {
    mmap(nullptr, kAllocateGranularity, PROT_READ | PROT_WRITE,
         MAP_ANONYMOUS | MAP_SHARED | MAP_POPULATE, -1, 0);
    mmap_times++;
    if (alloc_mem_traces) {
      alloc_mem_traces->emplace_back(microtime(),
                                     mmap_times * kAllocateGranularity);
    }
    struct sysinfo info;
    sysinfo(&info);
    if (info.freeram < free_mem_mbytes_target * nu::kOneMB) {
      break;
    }
  }
}

void wait_for_signal() {
  while (!rt::access_once(signalled)) {
    timer_sleep(100);
  }
  rt::access_once(signalled) = false;
}

void do_work() {
  std::cout << "clearing linux cache..." << std::endl;
  clear_linux_cache();
  std::cout << "working towards target 0..." << std::endl;
  do_mmap_until(kFreeMemMBTarget0, nullptr);
  std::cout << "waiting for signal..." << std::endl;

  wait_for_signal();

  std::cout << "working towards target 1..." << std::endl;
  bool done = false;
  std::vector<Trace> avail_mem_traces;
  std::vector<Trace> alloc_mem_traces;
  auto logging_thread = rt::Thread(
      [&avail_mem_traces, &done] { logging(done, &avail_mem_traces); });

  do_mmap_until(kFreeMemMBTarget1, &alloc_mem_traces);

  rt::access_once(done) = true;
  logging_thread.Join();

  std::cout << "waiting for signal..." << std::endl;
  wait_for_signal();

  std::cout << "writing traces..." << std::endl;
  {
    std::ofstream avail_ofs("avail_mem_traces", std::ofstream::trunc);
    for (auto [time_us, ram] : avail_mem_traces) {
      avail_ofs << time_us << " " << ram << std::endl;
    }
    std::ofstream alloc_ofs("alloc_mem_traces", std::ofstream::trunc);
    for (auto [time_us, ram] : alloc_mem_traces) {
      alloc_ofs << time_us << " " << ram << std::endl;
    }
  }
  std::cout << "done..." << std::endl;
}

void signal_handler(int signum) { rt::access_once(signalled) = true; }

int main(int argc, char **argv) {
  mlockall(MCL_CURRENT | MCL_FUTURE);
  signal(SIGHUP, signal_handler);

  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] { do_work(); });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

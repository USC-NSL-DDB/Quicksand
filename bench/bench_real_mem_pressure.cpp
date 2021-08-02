#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#define ACCESS_ONCE(x)                                                         \
  (*static_cast<std::remove_reference<decltype(x)>::type volatile *>(&(x)))

constexpr uint64_t kAllocateGranularity = 1ULL << 26;
constexpr uint32_t kAllocateIters = 150;
constexpr uint32_t kLoggingIntervalUs = 1000;

struct Trace {
  uint64_t time_us;
  uint64_t free_ram;
};

uint64_t Microtime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * (uint64_t)1000000 + tv.tv_usec;
}

void logging(const bool &done, std::vector<Trace> *traces) {
  auto last_poll_us = Microtime();
  while (!ACCESS_ONCE(done)) {
    auto next_poll_us = last_poll_us + kLoggingIntervalUs;
    while (Microtime() < next_poll_us)
      ;
    last_poll_us = Microtime();

    struct sysinfo info;
    sysinfo(&info);
    traces->emplace_back(Microtime(), info.freeram);
  }
}

void do_work() {
  std::cout << "Now let's start the second server. Press enter to continue."
            << std::endl;
  std::cin.ignore();

  bool done = false;
  std::vector<Trace> traces;
  auto logging_thread =
      std::thread([&traces, &done] { logging(done, &traces); });

  for (uint32_t i = 0; i < kAllocateIters; i++) {
    std::cout << Microtime() << " " << i << std::endl;
    mmap(nullptr, kAllocateGranularity, PROT_READ | PROT_WRITE,
         MAP_ANONYMOUS | MAP_SHARED | MAP_POPULATE, -1, 0);
  }

  ACCESS_ONCE(done) = true;
  logging_thread.join();

  std::ofstream ofs("log", std::ofstream::trunc);
  for (auto [time_us, free_mem] : traces) {
    ofs << time_us << " " << free_mem << std::endl;
  }
  while (1) {}
}

int main(int argc, char **argv) { do_work(); }

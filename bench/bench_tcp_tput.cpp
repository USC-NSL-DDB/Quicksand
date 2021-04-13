#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
#include <runtime/runtime.h>
#include <runtime/tcp.h>
#include <runtime/timer.h>
}
#include <thread.h>

#include "defs.hpp"
#include "utils/tcp.hpp"

using namespace nu;
using namespace std;

constexpr uint32_t kNumThreads = 300;
constexpr netaddr server_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 4),
                                 .port = 8080};

bool server_mode;

struct AlignedCnt {
  uint32_t cnt;
  uint8_t pads[kCacheLineBytes - sizeof(cnt)];
};

AlignedCnt cnts[kNumThreads];

void server_fn(tcpconn_t *c) {
  while (true) {
    uint8_t buf0[24];
    BUG_ON(!tcp_read_until(c, buf0, sizeof(buf0)));
    uint8_t buf1[16];
    uint8_t buf2[8];
    BUG_ON(!tcp_write2_until(c, buf1, sizeof(buf1), buf2, sizeof(buf2)));
  }
}

void do_server() {
  tcpqueue_t *q;
  BUG_ON(tcp_listen(server_addr, kNumThreads, &q) != 0);
  tcpconn_t *c;
  while (tcp_accept(q, &c) == 0) {
    rt::Thread([&, c] { server_fn(c); }).Detach();
  }
}

void do_client() {
  for (uint32_t i = 0; i < kNumThreads; i++) {
    rt::Thread([&, tid = i] {
      tcpconn_t *c;
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      BUG_ON(tcp_dial(local_addr, server_addr, &c) != 0);

      while (true) {
        uint8_t buf0[24];
        BUG_ON(!tcp_write_until(c, buf0, sizeof(buf0)));
        uint8_t buf1[16];
        uint8_t buf2[8];
        BUG_ON(!tcp_read_until(c, buf1, sizeof(buf1)));
        BUG_ON(!tcp_read_until(c, buf2, sizeof(buf2)));
        cnts[tid].cnt++;
      }
    }).Detach();
  }

  uint64_t old_sum = 0;
  uint64_t old_us = microtime();
  while (true) {
    timer_sleep(1000 * 1000);
    auto us = microtime();
    uint64_t sum = 0;
    for (uint32_t i = 0; i < kNumThreads; i++) {
      sum += ACCESS_ONCE(cnts[i].cnt);
    }
    std::cout << us - old_us << " " << sum - old_sum << std::endl;
    old_sum = sum;
    old_us = us;
  }
}

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;

  if (server_mode) {
    do_server();
  } else {
    do_client();
  }
}

int main(int argc, char **argv) {
  int ret;
  std::string mode_str;

  if (argc < 3) {
    goto wrong_args;
  }

  mode_str = std::string(argv[2]);
  if (mode_str == "SRV") {
    server_mode = true;
  } else if (mode_str == "CLT") {
    server_mode = false;
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
  std::cerr << "usage: [cfg_file] CLT/SRV" << std::endl;
  return -EINVAL;
}

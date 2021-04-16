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
#include "utils/bench.hpp"
#include "utils/tcp.hpp"

using namespace nu;
using namespace std;

constexpr static uint32_t kNumRuns = 100000;
constexpr netaddr server_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 4),
                                 .port = 8080};

bool server_mode;

uint8_t server_buf0[8];
uint8_t server_buf1[32];
uint8_t server_buf2[16];
uint8_t server_buf3[4];
uint8_t client_buf0[8];
uint8_t client_buf1[32];
uint8_t client_buf2[16];
uint8_t client_buf3[4];

void do_server() {
  tcpqueue_t *q;
  BUG_ON(tcp_listen(server_addr, 1, &q) != 0);
  tcpconn_t *c;
  while (tcp_accept(q, &c) == 0) {
    for (uint32_t i = 0; i < kNumRuns; i++) {
      BUG_ON(!tcp_read_until(c, server_buf0, sizeof(server_buf0)));
      BUG_ON(!tcp_read_until(c, server_buf1, sizeof(server_buf1)));
      BUG_ON(!tcp_write2_until(c, server_buf2, sizeof(server_buf2), server_buf3,
                               sizeof(server_buf3)));
    }
  }
}

void do_client() {
  tcpconn_t *c;
  netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  BUG_ON(tcp_dial(local_addr, server_addr, &c) != 0);

  std::vector<uint64_t> tscs;
  for (uint32_t i = 0; i < kNumRuns; i++) {
    auto start_tsc = rdtsc();
    BUG_ON(!tcp_write2_until(c, client_buf0, sizeof(client_buf0), client_buf1,
                             sizeof(client_buf1)));
    BUG_ON(!tcp_read_until(c, client_buf2, sizeof(client_buf2)));
    BUG_ON(!tcp_read_until(c, client_buf3, sizeof(client_buf3)));
    auto end_tsc = rdtsc();
    tscs.push_back(end_tsc - start_tsc);
  }
  print_percentile(&tscs);
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

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
#include <net.h>
#include <runtime.h>
#include <thread.h>

#include "defs.hpp"
#include "utils/bench.hpp"

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
  auto *q = rt::TcpQueue::Listen(server_addr, 1);
  BUG_ON(!q);

  rt::TcpConn *c;
  while ((c = q->Accept())) {
    for (uint32_t i = 0; i < kNumRuns; i++) {
      BUG_ON(c->ReadFull(server_buf0, sizeof(server_buf0)) <= 0);
      BUG_ON(c->ReadFull(server_buf1, sizeof(server_buf1)) <= 0);
      constexpr static iovec iovecs[] = {{server_buf2, sizeof(server_buf2)},
                                         {server_buf3, sizeof(server_buf3)}};
      BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
    }
  }
}

void do_client() {
  rt::TcpConn *c;
  netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  c = rt::TcpConn::Dial(local_addr, server_addr);
  BUG_ON(!c);

  std::vector<uint64_t> tscs;
  for (uint32_t i = 0; i < kNumRuns; i++) {
    auto start_tsc = rdtsc();
    constexpr static iovec iovecs[] = {{client_buf0, sizeof(client_buf0)},
                                       {client_buf1, sizeof(client_buf1)}};
    BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
    BUG_ON(c->ReadFull(client_buf2, sizeof(client_buf2)) <= 0);
    BUG_ON(c->ReadFull(client_buf3, sizeof(client_buf3)) <= 0);
    auto end_tsc = rdtsc();
    tscs.push_back(end_tsc - start_tsc);
  }
  print_percentile(&tscs);
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

  ret = rt::RuntimeInit(argv[1], [] {
    std::cout << "Running " << __FILE__ "..." << std::endl;

    if (server_mode) {
      do_server();
    } else {
      do_client();
    }
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV" << std::endl;
  return -EINVAL;
}

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

#include "nu/commons.hpp"

using namespace nu;
using namespace std;

constexpr uint32_t kNumThreads = 600;
constexpr netaddr server_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 2),
                                 .port = 8080};

bool server_mode;

struct alignas(64) Buffers {
  uint8_t server_buf0[8];
  uint8_t server_buf1[32];
  uint8_t server_buf2[16];
  uint8_t server_buf3[4];
  uint8_t client_buf0[8];
  uint8_t client_buf1[32];
  uint8_t client_buf2[16];
  uint8_t client_buf3[4];
} buffers[kNumThreads];

struct alignas(64) AlignedCnt {
  uint32_t cnt;
};

AlignedCnt cnts[kNumThreads];

void server_fn(rt::TcpConn *c, uint32_t tid) {
  while (true) {
    BUG_ON(c->ReadFull(buffers[tid].server_buf0,
                       sizeof(buffers[tid].server_buf0)) <= 0);
    BUG_ON(c->ReadFull(buffers[tid].server_buf1,
                       sizeof(buffers[tid].server_buf1)) <= 0);
    const iovec iovecs[] = {
        {buffers[tid].server_buf2, sizeof(buffers[tid].server_buf2)},
        {buffers[tid].server_buf3, sizeof(buffers[tid].server_buf3)}};
    BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  }
}

void do_server() {
  auto *q = rt::TcpQueue::Listen(server_addr, kNumThreads);
  BUG_ON(!q);

  rt::TcpConn *c;
  uint32_t tid = 0;
  while ((c = q->Accept())) {
    rt::Thread([&, c] { server_fn(c, tid++); }).Detach();
  }
}

void do_client() {
  for (uint32_t i = 0; i < kNumThreads; i++) {
    rt::Thread([&, tid = i] {
      rt::TcpConn *c;
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      c = rt::TcpConn::Dial(local_addr, server_addr);
      BUG_ON(!c);

      while (true) {
        const iovec iovecs[] = {
            {buffers[tid].client_buf0, sizeof(buffers[tid].client_buf0)},
            {buffers[tid].client_buf1, sizeof(buffers[tid].client_buf1)}};
        BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
        BUG_ON(c->ReadFull(buffers[tid].client_buf2,
                           sizeof(buffers[tid].client_buf2)) <= 0);
        BUG_ON(c->ReadFull(buffers[tid].client_buf3,
                           sizeof(buffers[tid].client_buf3)) <= 0);
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

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
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

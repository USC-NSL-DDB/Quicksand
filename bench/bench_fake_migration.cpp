#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <sys/mman.h>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/net.h>
#include <runtime/runtime.h>
#include <runtime/tcp.h>
#include <runtime/timer.h>
}
#include <sync.h>
#include <thread.h>

#include "defs.hpp"
#include "utils/tcp.hpp"

using namespace nu;
using namespace std;

constexpr netaddr server_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 4),
                                 .port = 8080};
constexpr uint32_t kBufSize = 6 << 20;
constexpr uint32_t kNumWorkers = 8;
bool server_mode;
void *buf;

enum ID { MAIN, WORKER };

using IDType = uint64_t;

void do_main(tcpconn_t *c) {
  auto buf = mmap(nullptr, kBufSize, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  rt::WaitGroup wg(kNumWorkers);
  auto *wg_p = &wg;
  BUG_ON(!tcp_write_until(c, &wg_p, sizeof(&wg_p)));
  auto t0 = rdtsc();
  ::buf = buf;
  wg.Wait();
  auto t1 = rdtsc();

  preempt_disable();
  std::cout << t1 - t0 << " " << t0 << " " << t1 << std::endl;
  preempt_enable();
}

void do_worker(tcpconn_t *c) {
  rt::WaitGroup *wg_p;
  BUG_ON(!tcp_read_until(c, &wg_p, sizeof(wg_p)));
  wg_p->Done();
}

void server_fn(tcpconn_t *c) {
  while (true) {
    IDType id;
    BUG_ON(!tcp_read_until(c, &id, sizeof(id)));
    if (id == MAIN) {
      do_main(c);
    } else if (id == WORKER) {
      do_worker(c);
    } else {
      BUG();
    }
  }
}

void do_server() {
  tcpqueue_t *q;
  BUG_ON(tcp_listen(server_addr, 100, &q) != 0);
  tcpconn_t *c;
  while (tcp_accept(q, &c) == 0) {
    rt::Thread([&, c] { server_fn(c); }).Detach();
  }
}

void do_client() {
  static uint8_t buf[kBufSize];
  tcpconn_t *c_main;
  tcpconn_t *c_workers[kNumWorkers];
  netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  BUG_ON(tcp_dial(local_addr, server_addr, &c_main) != 0);
  for (uint32_t i = 0; i < kNumWorkers; i++) {
    BUG_ON(tcp_dial(local_addr, server_addr, &c_workers[i]) != 0);
  }

  while (true) {
    IDType id = MAIN;
    BUG_ON(!tcp_write_until(c_main, &id, sizeof(id)));
    void *wg_p;
    BUG_ON(!tcp_read_until(c_main, &wg_p, sizeof(wg_p)));

    std::vector<rt::Thread> threads;
    for (uint32_t i = 0; i < kNumWorkers; i++) {
      threads.emplace_back([&, tid = i] {
        IDType id = WORKER;
        BUG_ON(!tcp_write2_until(c_workers[tid], &id, sizeof(id), &wg_p,
                                 sizeof(wg_p)));
        uint64_t len = kBufSize / kNumWorkers;
        uint64_t start_offset = tid * len;
        BUG_ON(!tcp_write2_until(c_workers[tid], &start_offset,
                                 sizeof(start_offset), &len, sizeof(len)));
        BUG_ON(!tcp_write_until(c_workers[tid], &buf[start_offset], len));
      });
    }
    for (auto &thread : threads) {
      thread.Join();
    }
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

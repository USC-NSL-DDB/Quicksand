#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>

#include <net.h>
#include <runtime.h>
#include <thread.h>

using namespace rt;

constexpr static uint32_t kPingPongTimes = 2000000;

bool is_server;

void run_server(bool poll) {
  netaddr laddr = {.ip = 0, .port = 8081};
  auto *q = TcpQueue::Listen(laddr, 1);
  BUG_ON(!q);
  TcpConn *c = q->Accept();
  for (uint32_t i = 0; i < kPingPongTimes; i++) {
    int num;
    BUG_ON(c->ReadFull(&num, sizeof(num), /* nt = */ false,
                       /* poll = */ poll) <= 0);
    num++;
    BUG_ON(c->WriteFull(&num, sizeof(num), /* nt = */ false,
                        /* poll = */ poll) < 0);
  }
  BUG_ON(c->Shutdown(SHUT_RDWR) != 0);
  delete c;
  q->Shutdown();
  delete q;
}

uint64_t run_client(bool poll) {
  netaddr laddr = {.ip = 0, .port = 0};
  netaddr raddr = {.ip = MAKE_IP_ADDR(18, 18, 1, 4), .port = 8081};
  auto *c = TcpConn::Dial(laddr, raddr);
  BUG_ON(!c);
  int num = 0;
  auto start_us = microtime();
  for (uint32_t i = 0; i < kPingPongTimes; i++) {
    BUG_ON(c->WriteFull(&num, sizeof(num), /* nt = */ false,
                        /* poll = */ poll) < 0);
    int new_num;
    BUG_ON(c->ReadFull(&new_num, sizeof(new_num), /* nt = */ false,
                       /* poll = */ poll) <= 0);
    BUG_ON(num + 1 != new_num);
    num = new_num;
  }
  auto end_us = microtime();
  BUG_ON(c->Shutdown(SHUT_RDWR) != 0);
  delete c;
  return end_us - start_us;
}

uint64_t run(bool poll) {
  auto server_thread = rt::Thread([&] { run_server(poll); });
  delay_us(100);

  uint64_t duration_us;
  auto client_thread = rt::Thread([&] { duration_us = run_client(poll); });

  server_thread.Join();
  client_thread.Join();

  return duration_us;
}

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
    auto t_no_poll = run(false);
    auto t_poll = run(true);

    if (t_no_poll < t_poll) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

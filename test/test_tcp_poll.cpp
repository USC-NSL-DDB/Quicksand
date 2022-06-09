#include <net.h>
#include <runtime.h>
#include <thread.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "nu/runtime.hpp"

using namespace rt;

constexpr static uint32_t kPingPongTimes = 400000;

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
  netaddr raddr = {.ip = get_cfg_ip(), .port = 8081};
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
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    run(true);
    std::cout << "Passed" << std::endl;
  });
}

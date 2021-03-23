#pragma once

#include <cstdint>
#include <vector>

extern "C" {
#include <runtime/tcp.h>
}

namespace nu {

class Migrator {
public:
  Migrator();
  ~Migrator();
  void run_loader_loop(uint16_t loader_port);
  void migrate(std::vector<void *> heaps);

private:
  constexpr static uint32_t kTCPListenBackLog = 64;
  tcpqueue_t *loader_tcp_queue_;

  void do_load(tcpconn_t *c);
};
} // namespace nu

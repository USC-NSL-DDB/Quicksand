extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}
#include "thread.h"

#include "heap_mgr.hpp"
#include "migrator.hpp"
#include "runtime_alloc.hpp"

namespace nu {

Migrator::Migrator() {}

void Migrator::do_load(tcpconn_t *c) {}

Migrator::~Migrator() {
  if (loader_tcp_queue_) {
    tcp_qshutdown(loader_tcp_queue_);
  }
  barrier();
  while (ACCESS_ONCE(loader_tcp_queue_))
    ;
}

void Migrator::run_loader_loop(uint16_t loader_port) {
  rt::Thread([&]() {
    netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = loader_port};
    BUG_ON(tcp_listen(addr, kTCPListenBackLog, &loader_tcp_queue_) != 0);
    tcpconn_t *c;
    while (tcp_accept(loader_tcp_queue_, &c) == 0) {
      do_load(c);
    }
    barrier();
    loader_tcp_queue_ = nullptr;
  })
      .Detach();
}

void Migrator::migrate(std::vector<void *> heaps) {
  for (auto heap : heaps) {
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap);
    ACCESS_ONCE(heap_header->migrating) = true;
    heap_header->rcu_lock.synchronize();
    auto all_threads = heap_header->threads->all_keys();
    for (auto thread : all_threads) {
      thread_mark_migration(thread);
    }
  }
  // TODO: migrate those heaps and the related threads.
}

} // namespace nu

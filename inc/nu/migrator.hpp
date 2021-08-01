#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_set>
#include <vector>

extern "C" {
#include <base/compiler.h>
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include <net.h>
#include <sync.h>

#include "nu/conn_mgr.hpp"
#include "nu/utils/netaddr.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

struct ObjRPCRespHdr;

class MigratorConnManager : public ConnectionManager<netaddr> {
public:
  MigratorConnManager();

private:
  static std::function<rt::TcpConn *(netaddr)> creator_;
};

enum MigratorRPC_t { COPY, MIGRATE, FORWARD, RESERVE_CONNS, UNMAP };

struct RPCReqReserveConns {
  uint32_t num;
  netaddr dest_server_addr;
} __attribute__((packed));

struct RPCReqCopy {
  uint64_t start_addr;
  uint64_t len;
  rt::WaitGroup *wg;
} __attribute__((packed));

struct HeapMmapPopulateTask {
  HeapRange range;
  bool mmapped;
  std::unique_ptr<rt::Mutex> mu;
  std::unique_ptr<rt::CondVar> cv;

  HeapMmapPopulateTask(HeapRange _range)
      : range(_range), mmapped(false), mu(new rt::Mutex()),
        cv(new rt::CondVar()) {}
};

class Migrator {
public:
  constexpr static uint32_t kDefaultNumReservedConns = 32;
  constexpr static uint32_t kTransmitHeapNumThreads = 2;
  constexpr static uint32_t kMigratorServerPort = 8002;

  ~Migrator();
  void run_loop();
  void migrate(Resource pressure, std::vector<HeapRange> heaps);
  void forward_to_original_server(rt::TcpConn *conn_to_client,
                                  uint64_t stack_top, const ObjRPCRespHdr &hdr,
                                  const void *payload);
  void reserve_conns(uint32_t num, netaddr dest_server_addr);

private:
  constexpr static uint32_t kTCPListenBackLog = 64;
  MigratorConnManager conn_mgr_;
  std::unique_ptr<rt::TcpQueue> tcp_queue_;

  void handle_copy(rt::TcpConn *c);
  void handle_load(rt::TcpConn *c);
  void handle_reserve_conns(rt::TcpConn *c);
  void handle_forward(rt::TcpConn *c);
  void handle_unmap(rt::TcpConn *c);
  VAddrRange load_stack_cluster_mmap_task(rt::TcpConn *c);
  void transmit(rt::TcpConn *c, HeapHeader *heap_header);
  void transmit_stack_cluster_mmap_task(rt::TcpConn *c);
  void transmit_heap(rt::TcpConn *c, HeapHeader *heap_header);
  void transmit_heap_mmap_populate_ranges(rt::TcpConn *c,
                                          const std::vector<HeapRange> &heaps);
  void transmit_mutexes(rt::TcpConn *c, HeapHeader *heap_header,
                        std::unordered_set<thread_t *> *mutex_threads);
  void transmit_condvars(rt::TcpConn *c, HeapHeader *heap_header,
                         std::unordered_set<thread_t *> *condvar_threads);
  void transmit_time(rt::TcpConn *c, HeapHeader *heap_header,
                     std::unordered_set<thread_t *> *time_threads);
  void transmit_threads(rt::TcpConn *c, const std::vector<thread_t *> &threads);
  void transmit_one_thread(rt::TcpConn *c, thread_t *thread);
  bool mark_migrating_threads(HeapHeader *heap_header);
  void unmap_destructed_heaps(rt::TcpConn *c,
                              std::vector<HeapHeader *> *destructed_heaps);
  void load(rt::TcpConn *c);
  void load_heap(rt::TcpConn *c, HeapMmapPopulateTask *task);
  std::vector<HeapRange> load_heap_mmap_populate_ranges(rt::TcpConn *c);
  void load_mutexes(rt::TcpConn *c, HeapHeader *heap_header);
  void load_condvars(rt::TcpConn *c, HeapHeader *heap_header);
  void load_time(rt::TcpConn *c, HeapHeader *heap_header);
  void load_threads(rt::TcpConn *c, HeapHeader *heap_header);
  thread_t *load_one_thread(rt::TcpConn *c, HeapHeader *heap_header);
  rt::Thread
  do_heap_mmap_populate(uint32_t old_server_ip,
                        const std::vector<HeapRange> &populate_ranges,
                        std::vector<HeapMmapPopulateTask> *populate_tasks);
};
} // namespace nu

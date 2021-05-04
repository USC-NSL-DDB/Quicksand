#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <unordered_set>

extern "C" {
#include <base/compiler.h>
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include <net.h>
#include <sync.h>

#include "conn_mgr.hpp"
#include "utils/netaddr.hpp"
#include "utils/slab.hpp"

namespace nu {

struct ObjRPCRespHdr;

class MigratorConnManager : public ConnectionManager<netaddr> {
public:
  MigratorConnManager();

private:
  static std::function<rt::TcpConn *(netaddr)> creator_;
};

enum MigratorRPC_t { LOAD, RESERVE_CONNS, COPY };

struct RPCReqReserveConns {
  uint32_t num;
  netaddr dest_server_addr;
} __attribute__((packed));

struct RPCReqCopy {
  uint64_t start_addr;
  uint64_t len;
  rt::WaitGroup *wg;
} __attribute__((packed));

class Migrator {
public:
  constexpr static uint32_t kDefaultNumReservedConns = 32;
  constexpr static uint32_t kTransmitHeapNumThreads = 3;

  ~Migrator();
  void run_loop(uint16_t port);
  void migrate(std::list<void *> heaps);
  void forward_to_original_server(const ObjRPCRespHdr &hdr, const void *payload,
                                  rt::TcpConn *conn_to_client);
  void reserve_conns(uint32_t num, netaddr dest_server_addr);

private:
  constexpr static uint32_t kTCPListenBackLog = 64;
  MigratorConnManager conn_mgr_;
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  rt::TcpConn *loader_conn_;
  rt::Mutex loader_mutex_;
  uint32_t remaining_forwarding_cnts_;
  rt::Mutex forwarding_mutex_;
  rt::CondVar loader_done_forwarding_;

  void handle_load(rt::TcpConn *c);
  void handle_reserve_conns(rt::TcpConn *c);
  void handle_copy(rt::TcpConn *c);
  void transmit_and_forward(netaddr dest_addr, void *heap_base);
  void transmit_heap(rt::TcpConn *c, netaddr dest_addr,
                     HeapHeader *heap_header);
  void parallel_transmit_heap(netaddr dest_addr, rt::WaitGroup *wg,
                              uint64_t start_addr, uint64_t len);
  void transmit_mutexes(rt::TcpConn *c, HeapHeader *heap_header,
                        std::unordered_set<thread_t *> *mutex_threads);
  void transmit_condvars(rt::TcpConn *c, HeapHeader *heap_header,
                         std::unordered_set<thread_t *> *condvar_threads);
  void transmit_time(rt::TcpConn *c, HeapHeader *heap_header,
                     std::unordered_set<thread_t *> *time_threads);
  void transmit_threads(rt::TcpConn *c, const std::vector<thread_t *> &threads);
  void transmit_one_thread(rt::TcpConn *c, thread_t *thread);
  void forward_to_client(uint32_t num_rpcs, rt::TcpConn *conn_to_new_server);
  void load(rt::TcpConn *c);
  void *load_heap(rt::TcpConn *c, rt::Mutex *loader_mutex);
  void load_mutexes(rt::TcpConn *c, HeapHeader *heap_header);
  void load_condvars(rt::TcpConn *c, HeapHeader *heap_header);
  void load_time(rt::TcpConn *c, HeapHeader *heap_header);
  void load_threads(rt::TcpConn *c, HeapHeader *heap_header);
  thread_t *load_one_thread(rt::TcpConn *c, HeapHeader *heap_header);
};
} // namespace nu

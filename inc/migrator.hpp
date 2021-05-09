#pragma once

#include <cstdint>
#include <functional>
#include <list>
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

enum MigratorRPC_t { MIGRATE, FORWARD, RESERVE_CONNS };

struct RPCReqReserveConns {
  uint32_t num;
  netaddr dest_server_addr;
} __attribute__((packed));

struct RPCReqCopy {
  uint64_t start_addr;
  uint64_t len;
  rt::WaitGroup *wg;
} __attribute__((packed));

struct HeapParam {
  HeapHeader *heap_header;
  int obj_ref_cnt;
  uint64_t start_addr;
  uint64_t len;
};

struct LoadHeapTask {
  HeapParam param;
  bool mmapped;
  std::unique_ptr<rt::Mutex> mu;
  std::unique_ptr<rt::CondVar> cv;

  LoadHeapTask(HeapParam _param)
      : param(_param), mmapped(false), mu(new rt::Mutex()),
        cv(new rt::CondVar()) {}
};

class Migrator {
public:
  constexpr static uint32_t kDefaultNumReservedConns = 32;
  constexpr static uint32_t kTransmitHeapNumThreads = 3;

  ~Migrator();
  void run_loop(uint16_t port);
  void migrate(Resource pressure, std::list<void *> heaps);
  void forward_to_original_server(const ObjRPCRespHdr &hdr, const void *payload,
                                  rt::TcpConn *conn_to_client);
  void reserve_conns(uint32_t num, netaddr dest_server_addr);

private:
  constexpr static uint32_t kTCPListenBackLog = 64;
  MigratorConnManager conn_mgr_;
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  uint16_t port_;

  void handle_load(rt::TcpConn *c);
  void handle_reserve_conns(rt::TcpConn *c);
  void handle_forward(rt::TcpConn *c);
  void transmit(rt::TcpConn *c, const HeapParam &heap_param);
  void transmit_heap(rt::TcpConn *c, const HeapParam &param);
  std::vector<HeapParam>
  transmit_all_heaps_params(rt::TcpConn *c, const std::list<void *> &heaps);
  void transmit_mutexes(rt::TcpConn *c, HeapHeader *heap_header,
                        std::unordered_set<thread_t *> *mutex_threads);
  void transmit_condvars(rt::TcpConn *c, HeapHeader *heap_header,
                         std::unordered_set<thread_t *> *condvar_threads);
  void transmit_time(rt::TcpConn *c, HeapHeader *heap_header,
                     std::unordered_set<thread_t *> *time_threads);
  void transmit_threads(rt::TcpConn *c, const std::vector<thread_t *> &threads);
  void transmit_one_thread(rt::TcpConn *c, thread_t *thread);
  bool mark_migrating_threads(HeapHeader *heap_header);
  void load(rt::TcpConn *c);
  void load_heap(rt::TcpConn *c, LoadHeapTask *task);
  std::vector<HeapParam> load_all_heaps_params(rt::TcpConn *c);
  void load_mutexes(rt::TcpConn *c, HeapHeader *heap_header);
  void load_condvars(rt::TcpConn *c, HeapHeader *heap_header);
  void load_time(rt::TcpConn *c, HeapHeader *heap_header);
  void load_threads(rt::TcpConn *c, HeapHeader *heap_header);
  thread_t *load_one_thread(rt::TcpConn *c, HeapHeader *heap_header);
  rt::Thread
  prepare_load_heap_tasks(uint32_t old_server_ip,
                          const std::vector<HeapParam> &heap_params,
                          std::vector<LoadHeapTask> *load_heap_tasks);
};
} // namespace nu

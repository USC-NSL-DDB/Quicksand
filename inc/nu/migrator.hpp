#pragma once

#include <cstdint>
#include <folly/Function.h>
#include <functional>
#include <memory>
#include <span>
#include <unordered_set>
#include <vector>

extern "C" {
#include <base/compiler.h>
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include <net.h>
#include <sync.h>

#include "nu/heap_mgr.hpp"
#include "nu/rpc_server.hpp"
#include "nu/utils/netaddr.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

class Mutex;
class CondVar;
class Time;
enum MigratorTCPOp_t { kCopyHeap, kMigrate, kUnmap, kEnablePoll, kDisablePoll };

struct RPCReqReserveConns {
  RPCReqType rpc_type = kReserveConns;
  netaddr dest_server_addr;
} __attribute__((packed));

struct RPCReqForward {
  RPCReqType rpc_type = kForward;
  RPCReturnCode rc;
  RPCReturner returner;
  uint64_t stack_top;
  uint64_t payload_len;
  uint8_t payload[0];
};

struct RPCReqMigrateThreadAndRetVal {
  RPCReqType rpc_type = kMigrateThreadAndRetVal;
  RPCReturnCode (*handler)(HeapHeader *, void *, uint64_t, uint8_t *);
  HeapHeader *dest_heap_header;
  void *dest_ret_val_ptr;
  uint64_t payload_len;
  uint8_t payload[0];
};

class MigratorConnManager;

class MigratorConn {
public:
  MigratorConn();
  ~MigratorConn();
  MigratorConn(const MigratorConn &) = delete;
  MigratorConn &operator=(const MigratorConn &) = delete;
  MigratorConn(MigratorConn &&);
  MigratorConn &operator=(MigratorConn &&);
  rt::TcpConn *get_tcp_conn();
  void release();

private:
  rt::TcpConn *tcp_conn_;
  netaddr addr_;
  MigratorConnManager *manager_;
  friend class MigratorConnManager;

  MigratorConn(rt::TcpConn *tcp_conn, netaddr addr,
               MigratorConnManager *manager);
};

class MigratorConnManager {
public:
  ~MigratorConnManager();
  MigratorConn get(netaddr addr);

private:
  rt::Spin spin_;
  std::unordered_map<netaddr, std::stack<rt::TcpConn *>> pool_map_;
  friend class MigratorConn;

  void put(netaddr addr, rt::TcpConn *tcp_conn);
};

class Migrator {
public:
  constexpr static uint32_t kTransmitHeapNumThreads = 2;
  constexpr static uint32_t kDefaultNumReservedConns = 8;
  constexpr static uint32_t kPort = 8002;

  ~Migrator();
  void run_loop();
  void migrate(Resource resource, std::vector<HeapRange> heaps);
  void reserve_conns(netaddr dest_server_addr);
  void forward_to_original_server(RPCReturnCode rc, RPCReturner *returner,
                                  uint64_t payload_len, const void *payload);
  void forward_to_client(RPCReqForward &req);
  template <typename RetT>
  static void migrate_thread_and_ret_val(RPCReturnBuffer &&return_buf,
                                         RemObjID dest_id,
                                         RetT *dest_ret_val_ptr,
                                         folly::Function<void()> cleanup_fn);
  template <typename RetT>
  static RPCReturnCode load_thread_and_ret_val(HeapHeader *dest_heap_header,
                                               void *raw_dest_ret_val_ptr,
                                               uint64_t payload_len,
                                               uint8_t *payload);

private:
  constexpr static uint32_t kTCPListenBackLog = 64;
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  MigratorConnManager migrator_conn_mgr_;

  void handle_copy_heap(rt::TcpConn *c);
  void handle_load(rt::TcpConn *c);
  void handle_unmap(rt::TcpConn *c);
  VAddrRange load_stack_cluster_mmap_task(rt::TcpConn *c);
  void transmit(rt::TcpConn *c, HeapHeader *heap_header,
                struct list_head *head);
  void transmit_stack_cluster_mmap_task(rt::TcpConn *c);
  void transmit_heap(rt::TcpConn *c, HeapHeader *heap_header);
  void transmit_heap_mmap_populate_ranges(rt::TcpConn *c,
                                          const std::vector<HeapRange> &heaps);
  void transmit_mutexes(rt::TcpConn *c, std::vector<Mutex *> mutexes);
  void transmit_condvars(rt::TcpConn *c, std::vector<CondVar *> condvars);
  void transmit_time(rt::TcpConn *c, Time *time);
  void transmit_threads(rt::TcpConn *c, const std::vector<thread_t *> &threads);
  void transmit_one_thread(rt::TcpConn *c, thread_t *thread);
  bool try_mark_heap_migrating(HeapHeader *heap_header);
  void unmap_destructed_heaps(rt::TcpConn *c,
                              std::vector<HeapHeader *> *destructed_heaps);
  void load(rt::TcpConn *c);
  void load_heap(rt::TcpConn *c, HeapHeader *heap_header);
  std::vector<HeapRange> load_heap_mmap_populate_ranges(rt::TcpConn *c);
  void load_mutexes(rt::TcpConn *c, HeapHeader *heap_header);
  void load_condvars(rt::TcpConn *c, HeapHeader *heap_header);
  void load_time(rt::TcpConn *c, HeapHeader *heap_header);
  void load_threads(rt::TcpConn *c, HeapHeader *heap_header);
  thread_t *load_one_thread(rt::TcpConn *c, HeapHeader *heap_header);
  void init_aux_handlers(netaddr dest_addr);
  void finish_aux_handlers();
};

} // namespace nu

#include "nu/impl/migrator.ipp"

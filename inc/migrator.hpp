#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <unordered_set>

extern "C" {
#include <base/compiler.h>
#include <runtime/net.h>
#include <runtime/tcp.h>
}
#include "sync.h"

#include "conn_mgr.hpp"
#include "utils/slab.hpp"

bool operator==(netaddr x, netaddr y);

namespace std {
template <> struct hash<netaddr> {
  std::size_t operator()(const netaddr &k) const;
};
} // namespace std

namespace nu {

struct ObjRPCRespHdr;

class MigratorConnManager : public ConnectionManager<netaddr> {
public:
  MigratorConnManager();

private:
  static std::function<tcpconn_t *(netaddr)> creator_;
};

class Migrator {
public:
  ~Migrator();
  void run_loader_loop(uint16_t loader_port);
  void migrate(std::list<void *> heaps);
  void forward_to_original_server(const ObjRPCRespHdr &hdr, const void *payload,
                                  tcpconn_t *conn_to_client);

private:
  constexpr static uint32_t kTCPListenBackLog = 64;
  MigratorConnManager conn_mgr_;
  tcpqueue_t *loader_tcp_queue_;
  tcpconn_t *loader_conn_;
  rt::Mutex loader_mutex_;
  uint32_t remaining_forwarding_cnts_;
  rt::Mutex forwarding_mutex_;
  rt::CondVar loader_done_forwarding_;

  void transmit_and_forward(netaddr dest_addr, void *heap_base);
  void transmit_heap(tcpconn_t *c, HeapHeader *heap_header);
  void transmit_mutexes(tcpconn_t *c, HeapHeader *heap_header,
                        std::unordered_set<thread_t *> *mutex_threads);
  void transmit_condvars(tcpconn_t *c, HeapHeader *heap_header,
                         std::unordered_set<thread_t *> *condvar_threads);
  void transmit_time(tcpconn_t *c, HeapHeader *heap_header,
                     std::unordered_set<thread_t *> *time_threads);
  void transmit_threads(tcpconn_t *c, const std::vector<thread_t *> &threads);
  void transmit_one_thread(tcpconn_t *c, thread_t *thread);
  void forward_to_client(uint32_t num_rpcs, tcpconn_t *conn_to_new_server);
  void load(tcpconn_t *c);
  void *load_heap(tcpconn_t *c, rt::Mutex *loader_mutex);
  void load_mutexes(tcpconn_t *c, HeapHeader *heap_header);
  void load_condvars(tcpconn_t *c, HeapHeader *heap_header);
  void load_time(tcpconn_t *c, HeapHeader *heap_header);
  void load_threads(tcpconn_t *c, HeapHeader *heap_header);
  thread_t *load_one_thread(tcpconn_t *c, HeapHeader *heap_header);
};
} // namespace nu

#include "impl/migrator.ipp"

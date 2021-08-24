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

#include "nu/rpc_client_mgr.hpp"
#include "nu/utils/netaddr.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

enum MigratorRPC_t {
  kFetch,
  kForward,
  kReserveConn,
  kLoadMutexesInfo,
  kLoadMutexThreadInfo,
  kLoadCondvarsInfo,
  kLoadCondvarThreadInfo,
  kLoadTimeInfo,
  kLoadUnblockedThreads,
  kMigrate,
  kMap,
  kUnmap,
};

struct RPCReqReserveConn {
  MigratorRPC_t rpc_type = kReserveConn;
  netaddr dest_server_addr;
} __attribute__((packed));

struct RPCReqFetch {
  MigratorRPC_t rpc_type = kFetch;
  uint64_t start_addr;
  uint64_t len;
} __attribute__((packed));

struct HeapMmapPopulateTask {
  HeapRange range;
  bool mmapped;
  std::unique_ptr<rt::Mutex> mu;
  std::unique_ptr<rt::CondVar> cv;

  HeapMmapPopulateTask() {}
  HeapMmapPopulateTask(HeapRange r)
      : range(r), mmapped(false), mu(new rt::Mutex()), cv(new rt ::CondVar()) {}
};

struct RPCReqUnmap {
  MigratorRPC_t rpc_type = kUnmap;
  VAddrRange stack_cluster;
  uint64_t num_heaps;
  HeapMmapPopulateTask *map_task_descs;
  HeapHeader *heaps[0];
} __attribute__((packed));

struct RPCReqMigrate {
  MigratorRPC_t rpc_type = kMigrate;
  uint32_t src_ip;
  VAddrRange stack_cluster;
  HeapHeader *heap;
  HeapMmapPopulateTask *map_task_desc;
} __attribute__((packed));

struct RPCReqMap {
  MigratorRPC_t rpc_type = kMap;
  uint32_t src_ip;
  VAddrRange stack_cluster;
  uint32_t num_heaps;
  HeapRange ranges[0];
} __attribute__((packed));

struct RPCRespMap {
  HeapMmapPopulateTask *map_task_descs;
};

struct ThreadInfo {
  thread_t *th;
  void *trap_frame;
  uint64_t trap_frame_size;
  uint64_t stack_start;
  uint64_t stack_len;
};

struct MutexInfo {
  Mutex *mutex;
  bool has_blocked_threads;
};

struct CondvarInfo {
  CondVar *condvar;
  bool has_blocked_threads;
};

struct TimerEntryInfo {
  timer_entry *entry;
  ThreadInfo thread_info;
};

struct RPCReqLoadMutexesInfo {
  MigratorRPC_t rpc_type = kLoadMutexesInfo;
  HeapHeader *heap_header;
} __attribute__((packed));

struct RPCRespLoadMutexesInfo {
  MutexInfo mutex_infos[0];
};

struct RPCReqLoadCondvarsInfo {
  MigratorRPC_t rpc_type = kLoadCondvarsInfo;
  HeapHeader *heap_header;
} __attribute__((packed));

struct RPCRespLoadCondvarsInfo {
  CondvarInfo condvar_infos[0];
};

struct RPCReqLoadTimeInfo {
  MigratorRPC_t rpc_type = kLoadTimeInfo;
  HeapHeader *heap_header;
} __attribute__((packed));

struct RPCRespLoadTimeInfo {
  int64_t sum_tsc;
  uint32_t num_entries;
  TimerEntryInfo timer_entry_infos[0];
};

struct RPCReqLoadMutexThreadInfo {
  MigratorRPC_t rpc_type = kLoadMutexThreadInfo;
  Mutex *mutex;
} __attribute__((packed));

struct RPCRespLoadMutexThreadInfo {
  ThreadInfo thread_infos[0];
};

struct RPCReqLoadCondvarThreadInfo {
  MigratorRPC_t rpc_type = kLoadCondvarThreadInfo;
  CondVar *condvar;
} __attribute__((packed));

struct RPCRespLoadCondvarThreadInfo {
  ThreadInfo thread_infos[0];
};

struct RPCReqLoadUnblockedThreads {
  MigratorRPC_t rpc_type = kLoadUnblockedThreads;
  HeapHeader *heap_header;
  uint32_t num_blocked_threads;
  thread_t *threads[0];
} __attribute__((packed));

struct RPCRespLoadUnblockedThreads {
  ThreadInfo thread_infos[0];
};

struct RPCReqForward {
  MigratorRPC_t rpc_type = kForward;
  RPCReturnCode rc;
  RPCReturner returner;
  uint64_t stack_top;
  uint64_t payload_len;
  uint8_t payload[0];
};

class Migrator {
public:
  constexpr static uint32_t kLoadHeapNumThreads = 2;
  constexpr static uint32_t kMigratorServerPort = 8002;

  Migrator();
  ~Migrator();
  void run_loop();
  void migrate_heaps(Resource pressure, std::vector<HeapRange> heaps);

private:
  RPCClientMgr rpc_client_mgr_;

  void handle_req(std::span<std::byte> args, RPCReturner *returner);
  void handle_fetch(const RPCReqFetch &req, RPCReturner *returner);
  void handle_reserve_conn(const RPCReqReserveConn &req);
  void handle_forward(RPCReqForward &req);
  std::unique_ptr<RPCRespMap> handle_map(const RPCReqMap &req);
  void handle_migrate(const RPCReqMigrate &req);
  void handle_load_mutexes_info(const RPCReqLoadMutexesInfo &req,
                                RPCReturner *returner);
  void handle_load_mutex_thread_info(const RPCReqLoadMutexThreadInfo &req,
                                     RPCReturner *returner);
  void handle_load_condvars_info(const RPCReqLoadCondvarsInfo &req,
                                 RPCReturner *returner);
  void handle_load_condvar_thread_info(const RPCReqLoadCondvarThreadInfo &req,
                                       RPCReturner *returner);
  void handle_load_time_info(const RPCReqLoadTimeInfo &req,
                             RPCReturner *returner);
  void handle_load_unblocked_threads(const RPCReqLoadUnblockedThreads &req,
                                     RPCReturner *returner);
  void handle_unmap(const RPCReqUnmap &req);
  bool mark_migrating_threads(HeapHeader *heap_header);
  void unmap_destructed_heaps(RPCClient *client,
                              const std::vector<HeapHeader *> &destructed_heap,
                              HeapMmapPopulateTask *map_task_descs);
  void load_heap(RPCClient *client, HeapMmapPopulateTask *task);
  void load_mutexes(RPCClient *client, HeapHeader *heap_header,
                    std::vector<thread_t *> *blocked_threads);
  void load_condvars(RPCClient *client, HeapHeader *heap_header,
                     std::vector<thread_t *> *blocked_threads);
  void load_time(RPCClient *client, HeapHeader *heap_header,
                 std::vector<thread_t *> *blocked_threads);
  void load_unblocked_threads(RPCClient *client, HeapHeader *heap_header,
                              const std::vector<thread_t *> blocked_threads);
  thread_t *load_one_thread(RPCClient *client, const ThreadInfo &thread_info,
                            HeapHeader *heap_header);
  HeapMmapPopulateTask *pre_mmap(RPCClient *client,
                                 const std::vector<HeapRange> &heaps);
  void migrate(RPCClient *client, HeapHeader *heap_header,
               HeapMmapPopulateTask *map_task_desc);
  void fetch(RPCClient *client, uint64_t src_addr, uint64_t dest_addr,
             uint64_t len);
  void mmap_populate_heaps(HeapMmapPopulateTask *map_task_descs,
                           uint32_t num_descs, uint32_t old_server_ip);
  ThreadInfo acquire_thread_info(thread_t *th);
};

} // namespace nu

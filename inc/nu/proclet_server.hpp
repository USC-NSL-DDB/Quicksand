#pragma once

#include <cstdint>
#include <memory>
#include <sstream>

extern "C" {
#include <runtime/net.h>
}
#include <sync.h>

#include "nu/proclet_mgr.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/rpc.hpp"
#include "nu/utils/trace_logger.hpp"

namespace nu {

class ProcletServer {
 public:
  ProcletServer();
  ~ProcletServer();
  netaddr get_addr() const;
  template <typename Cls>
  static void update_ref_cnt(cereal::BinaryInputArchive &ia,
                             RPCReturner *returner);
  template <typename Cls>
  static void update_ref_cnt_locally(MigrationGuard *callee_guard,
                                     ProcletHeader *caller_header,
                                     ProcletHeader *callee_header, int delta);
  template <typename Cls, typename... As>
  static void construct_proclet(cereal::BinaryInputArchive &ia,
                                RPCReturner *returner);
  template <typename Cls, typename... As>
  static void construct_proclet_locally(MigrationGuard &&caller_guard,
                                        void *base, uint64_t size, bool pinned,
                                        As &&... args);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static void run_closure(cereal::BinaryInputArchive &ia,
                          RPCReturner *returner);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static MigrationGuard run_closure_locally(
      MigrationGuard *callee_migration_guard,
      const ProcletSlabGuard &callee_slab_guard, RetT *caller_ptr,
      ProcletHeader *caller_header, ProcletHeader *callee_header, FnPtr fn_ptr,
      S1s &&... states);

 private:
  using GenericHandler = void (*)(cereal::BinaryInputArchive &ia,
                                  RPCReturner *returner);

  TraceLogger trace_logger_;
  friend class RPCServer;

  static void forward(RPCReturnCode rc, RPCReturner *returner,
                      const void *payload, uint64_t payload_len);
  static void send_rpc_resp_ok(ArchivePool<>::OASStream *oa_sstream,
                               RPCReturner *returner);
  static void send_rpc_resp_wrong_client(RPCReturner *returner);
  void parse_and_run_handler(std::span<std::byte> args, RPCReturner *returner);
  template <typename Cls, typename... As>
  static void __construct_proclet(MigrationGuard *callee_guard, Cls *obj,
                                  cereal::BinaryInputArchive &ia,
                                  RPCReturner returner);
  template <typename Cls>
  static void __update_ref_cnt(MigrationGuard *callee_guard, Cls *obj,
                               RPCReturner returner, int delta,
                               bool *destructed);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static void __run_closure(MigrationGuard *callee_guard, Cls *obj,
                            cereal::BinaryInputArchive &ia,
                            RPCReturner returner);
  static void release_proclet(VAddrRange vaddr_range);
};
}  // namespace nu

#include "nu/impl/proclet_server.ipp"

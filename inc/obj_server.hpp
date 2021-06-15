#pragma once

#include <cereal/archives/binary.hpp>
#include <cstdint>
#include <memory>
#include <sstream>

extern "C" {
#include <runtime/tcp.h>
}
#include <net.h>

#include "utils/trace_logger.hpp"

namespace nu {

enum { OK = 0, FORWARDED, CLIENT_RETRY };

struct ObjRPCRespHdr {
  uint8_t rc;
  uint64_t payload_size;
};

class ObjServer {
public:
  constexpr static uint32_t kTCPListenBackLog = 64;

  ObjServer();
  ~ObjServer();
  ObjServer(uint16_t port);
  void init(uint16_t port);
  netaddr get_addr() const;
  void run_loop();
  template <typename Cls>
  static void update_ref_cnt(cereal::BinaryInputArchive &ia,
                             rt::TcpConn *rpc_conn);
  template <typename Cls>
  static void update_ref_cnt_locally(RemObjID id, int delta);
  template <typename Cls, typename... As>
  static void construct_obj(cereal::BinaryInputArchive &ia,
                            rt::TcpConn *rpc_conn);
  template <typename Cls, typename... As>
  static void construct_obj_locally(void *base, bool pinned, As &&... args);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static void run_closure(cereal::BinaryInputArchive &ia,
                          rt::TcpConn *rpc_conn);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static RetT run_closure_locally(RemObjID id, FnPtr fn_ptr, S1s &&... states);

private:
  using GenericHandler = void (*)(cereal::BinaryInputArchive &ia,
                                  rt::TcpConn *rpc_conn);
  uint16_t port_;
  std::unique_ptr<rt::TcpQueue> tcp_queue_;
  TraceLogger trace_logger_;
  friend class Migrator;

  static void send_rpc_resp(auto &ss, rt::TcpConn *rpc_conn);
  static void send_rpc_client_retry(rt::TcpConn *rpc_conn);
  void handle_reqs(rt::TcpConn *rpc_conn);
  template <typename Cls>
  static void __update_ref_cnt(Cls &obj, rt::TcpConn *rpc_conn,
                               HeapHeader *heap_header, int delta,
                               bool *deallocate);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static void __run_closure(Cls &obj, cereal::BinaryInputArchive &ia,
                            rt::TcpConn *rpc_conn);
};
} // namespace nu

#include "impl/obj_server.ipp"

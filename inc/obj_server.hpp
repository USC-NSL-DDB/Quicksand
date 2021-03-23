#pragma once

extern "C" {
#include <runtime/tcp.h>
}

#include <cereal/archives/binary.hpp>
#include <cstdint>

namespace nu {

class ObjServer {
public:
  constexpr static uint32_t kTCPListenBackLog = 64;

  ObjServer();
  ~ObjServer();
  ObjServer(uint16_t port);
  void init(uint16_t port);
  void run_loop();
  static void update_ref_cnt(cereal::BinaryInputArchive &ia,
                             cereal::BinaryOutputArchive &oa);
  template <typename Cls, typename... As>
  static void construct_obj(cereal::BinaryInputArchive &ia,
                            cereal::BinaryOutputArchive &oa);
  template <typename Cls, typename RetT, typename FnPtr, typename... S1s>
  static void closure_handler(cereal::BinaryInputArchive &ia,
                              cereal::BinaryOutputArchive &oa);
  template <typename Cls, typename RetT, typename MdPtr, typename... A1s>
  static void method_handler(cereal::BinaryInputArchive &ia,
                             cereal::BinaryOutputArchive &oa);

private:
  using GenericHandler = void (*)(cereal::BinaryInputArchive &ia,
                                  cereal::BinaryOutputArchive &oa);
  tcpqueue_t *tcp_queue_;

  void handle_reqs(tcpconn_t *c);
};
} // namespace nu

#include "impl/obj_server.ipp"

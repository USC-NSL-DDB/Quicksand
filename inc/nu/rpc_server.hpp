#pragma once

#include <cstdint>

#include "nu/utils/rpc.hpp"

namespace nu {

enum RPCReqEnum {
  // Migrator
  kReserveConns,
  kForward,
  kMigrateThreadAndRetVal,
  // Controller
  kRegisterNode,
  kVerifyMD5,
  kAllocateProclet,
  kDestroyProclet,
  kResolveProclet,
  kGetMigrationDest,
  kUpdateLocation,
  kReportFreeResource,
  // Proclet server,
  kProcletCall,
};

using RPCReqType = uint8_t;

class RPCServer {
 public:
  constexpr static uint32_t kPort = 12345;

  void run_background_loop();
};

}  // namespace nu

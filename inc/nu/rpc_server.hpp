#pragma once

#include <cstdint>

#include "nu/utils/rpc.hpp"

namespace nu {

enum RPCReqEnum {
  // Migrator
  kReserveConns,
  kForward,
  kMigrateCalleeBack,
  // Controller
  kRegisterNode,
  kAllocateObj,
  kDestroyObj,
  kResolveObj,
  kGetMigrationDest,
  kUpdateLocation,
  // Object server,
  kRemObjCall,
};

using RPCReqType = uint8_t;

class RPCServer {
public:
  constexpr static uint32_t kPort = 12345;

  void run_loop();
};

} // namespace nu

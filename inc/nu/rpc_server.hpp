#pragma once

#include <cstdint>

namespace nu {

enum RPCReqEnum {
  // Migrator
  kReserveConns,
  kForward,
  kMigrateThreadAndRetVal,
  // Controller
  kRegisterNode,
  kAllocateProclet,
  kDestroyProclet,
  kResolveProclet,
  kAcquireMigrationDest,
  kReleaseMigrationDest,
  kUpdateLocation,
  kReportFreeResource,
  // Proclet server,
  kProcletCall,
  kGCStack
};

using RPCReqType = uint8_t;

class RPCServer {
 public:
  constexpr static uint32_t kPort = 12345;

  RPCServer();

 private:
  void run_background_loop();
};

}  // namespace nu

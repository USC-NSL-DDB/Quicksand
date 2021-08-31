#include "nu/rpc_server.hpp"
#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/obj_server.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

void RPCServer::run_loop() {
  RPCServerInit(kPort, [](std::span<std::byte> args, RPCReturner *returner) {
    auto &rpc_type = from_span<RPCReqType>(args);

    switch (rpc_type) {
      // Migrator
    case kFetch: {
      auto &req = from_span<RPCReqFetch>(args);
      Runtime::migrator->handle_fetch(req, returner);
      break;
    }
    case kMigrate: {
      auto &req = from_span<RPCReqMigrate>(args);
      Runtime::migrator->handle_migrate(req);
      returner->Return(kOk);
      break;
    }
    case kLoadMutexesInfo: {
      auto &req = from_span<RPCReqLoadMutexesInfo>(args);
      Runtime::migrator->handle_load_mutexes_info(req, returner);
      break;
    }
    case kLoadMutexThreadInfo: {
      auto &req = from_span<RPCReqLoadMutexThreadInfo>(args);
      Runtime::migrator->handle_load_mutex_thread_info(req, returner);
      break;
    }
    case kLoadCondvarsInfo: {
      auto &req = from_span<RPCReqLoadCondvarsInfo>(args);
      Runtime::migrator->handle_load_condvars_info(req, returner);
      break;
    }
    case kLoadCondvarThreadInfo: {
      auto &req = from_span<RPCReqLoadCondvarThreadInfo>(args);
      Runtime::migrator->handle_load_condvar_thread_info(req, returner);
      break;
    }
    case kLoadTimeInfo: {
      auto &req = from_span<RPCReqLoadTimeInfo>(args);
      Runtime::migrator->handle_load_time_info(req, returner);
      break;
    }
    case kLoadUnblockedThreads: {
      auto &req = from_span<RPCReqLoadUnblockedThreads>(args);
      Runtime::migrator->handle_load_unblocked_threads(req, returner);
      break;
    }
    case kForward: {
      auto &req = from_span<RPCReqForward>(args);
      Runtime::migrator->handle_forward(req);
      returner->Return(kOk);
      break;
    }
    case kReserveConn: {
      auto &req = from_span<RPCReqReserveConn>(args);
      Runtime::migrator->handle_reserve_conn(req);
      returner->Return(kOk);
      break;
    }
    case kMap: {
      auto &req = from_span<RPCReqMap>(args);
      auto resp = Runtime::migrator->handle_map(req);
      auto span = to_span(*resp);
      returner->Return(kOk, span, [resp = std::move(resp)] {});
      break;
    }
    case kUnmap: {
      auto &req = from_span<RPCReqUnmap>(args);
      Runtime::migrator->handle_unmap(req);
      returner->Return(kOk);
      break;
    }
      // Controller
    case kRegisterNode: {
      auto &req = from_span<RPCReqRegisterNode>(args);
      auto resp = Runtime::controller_server->handle_register_node(req);
      auto span = to_span(*resp);
      returner->Return(kOk, span, [resp = std::move(resp)] {});
      break;
    }
    case kAllocateObj: {
      auto &req = from_span<RPCReqAllocateObj>(args);
      auto resp = Runtime::controller_server->handle_allocate_obj(req);
      auto span = to_span(*resp);
      returner->Return(kOk, span, [resp = std::move(resp)] {});
      break;
    }
    case kDestroyObj: {
      auto &req = from_span<RPCReqDestroyObj>(args);
      auto resp = Runtime::controller_server->handle_destroy_obj(req);
      auto span = to_span(*resp);
      returner->Return(kOk, span, [resp = std::move(resp)] {});
      break;
    }
    case kResolveObj: {
      auto &req = from_span<RPCReqResolveObj>(args);
      auto resp = Runtime::controller_server->handle_resolve_obj(req);
      auto span = to_span(*resp);
      returner->Return(kOk, span, [resp = std::move(resp)] {});
      break;
    }
    case kGetMigrationDest: {
      auto &req = from_span<RPCReqGetMigrationDest>(args);
      auto resp = Runtime::controller_server->handle_get_migration_dest(req);
      auto span = to_span(*resp);
      returner->Return(kOk, span, [resp = std::move(resp)] {});
      break;
    }
    case kUpdateLocation: {
      auto &req = from_span<RPCReqUpdateLocation>(args);
      Runtime::controller_server->handle_update_location(req);
      returner->Return(kOk);
      break;
    }
      // Object server
    case kRemObjCall: {
      args = args.subspan(sizeof(RPCReqType));
      Runtime::obj_server->parse_and_run_handler(args, returner);
      break;
    }
    default:
      BUG();
    }
  });
}

} // namespace nu

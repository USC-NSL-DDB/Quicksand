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
    case kReserveConns: {
      auto &req = from_span<RPCReqReserveConns>(args);
      Runtime::migrator->reserve_conns(req.dest_server_addr);
      returner->Return(kOk);
      break;
    }
    case kForward: {
      auto &req = from_span<RPCReqForward>(args);
      Runtime::migrator->forward_to_client(req);
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

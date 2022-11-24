#include "nu/rpc_server.hpp"

#include "nu/commons.hpp"
#include "nu/runtime.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/proclet_server.hpp"
#include "nu/utils/rpc.hpp"

namespace nu {

void RPCServer::run_background_loop() {
  auto rpc_handler = [](std::span<std::byte> args, RPCReturner *returner) {
    auto &rpc_type = from_span<RPCReqType>(args);

    switch (rpc_type) {
      // Migrator
      case kReserveConns: {
        auto &req = from_span<RPCReqReserveConns>(args);
        get_runtime()->reserve_conns(req.dest_server_ip);
        returner->Return(kOk);
        break;
      }
      case kForward: {
        auto &req = from_span<RPCReqForward>(args);
        get_runtime()->migrator()->forward_to_client(req);
        returner->Return(kOk);
        break;
      }
      case kMigrateThreadAndRetVal: {
        auto &req = from_span<RPCReqMigrateThreadAndRetVal>(args);
        auto rc = req.handler(req.dest_proclet_header, req.dest_ret_val_ptr,
                              req.payload_len, req.payload);
        returner->Return(rc);
        break;
      }
      // Controller
      case kRegisterNode: {
        auto &req = from_span<RPCReqRegisterNode>(args);
        auto resp =
            get_runtime()->controller_server()->handle_register_node(req);
        auto span = to_span(*resp);
        returner->Return(kOk, span, [resp = std::move(resp)] {});
        break;
      }
      case kVerifyMD5: {
        auto &req = from_span<RPCReqVerifyMD5>(args);
        auto resp = get_runtime()->controller_server()->handle_verify_md5(req);
        auto span = to_span(*resp);
        returner->Return(kOk, span, [resp = std::move(resp)] {});
        break;
      }
      case kAllocateProclet: {
        auto &req = from_span<RPCReqAllocateProclet>(args);
        auto resp =
            get_runtime()->controller_server()->handle_allocate_proclet(req);
        auto span = to_span(*resp);
        returner->Return(kOk, span, [resp = std::move(resp)] {});
        break;
      }
      case kDestroyProclet: {
        auto &req = from_span<RPCReqDestroyProclet>(args);
        auto resp =
            get_runtime()->controller_server()->handle_destroy_proclet(req);
        auto span = to_span(*resp);
        returner->Return(kOk, span, [resp = std::move(resp)] {});
        break;
      }
      case kResolveProclet: {
        auto &req = from_span<RPCReqResolveProclet>(args);
        auto resp =
            get_runtime()->controller_server()->handle_resolve_proclet(req);
        auto span = to_span(*resp);
        returner->Return(kOk, span, [resp = std::move(resp)] {});
        break;
      }
      // Proclet server
      case kProcletCall: {
        args = args.subspan(sizeof(RPCReqType));
        get_runtime()->proclet_server()->parse_and_run_handler(args, returner);
        break;
      }
      case kGCStack: {
        auto &req = from_span<RPCReqGCStack>(args);
        get_runtime()->stack_manager()->put(req.stack);
        returner->Return(kOk);
        break;
      }
      default:
        BUG();
    }
  };

  RPCServerInit(kPort, rpc_handler, /* blocking = */ false);
}

}  // namespace nu

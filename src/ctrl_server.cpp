extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}

#include <algorithm>

#include <runtime.h>
#include <sync.h>
#include <timer.h>

#include "nu/ctrl_server.hpp"

namespace nu {

ControllerServer::ControllerServer()
    : num_register_node_(0), num_verify_md5_(0), num_allocate_obj_(0),
      num_destroy_obj_(0), num_resolve_obj_(0), num_get_migration_dest_(0),
      num_update_location_(0), done_(false) {
  if constexpr (kEnableLogging) {
    logging_thread_ = rt::Thread([&] {
      std::cout << "register_node verify_md5 allocate_obj destroy_obj "
                   "resolve_obj get_migration_dest update_location"
                << std::endl;
      while (!rt::access_once(done_)) {
        timer_sleep(kPrintIntervalUs);
        std::cout << num_register_node_ << " " << num_verify_md5_ << " "
                  << num_allocate_obj_ << " " << num_destroy_obj_ << " "
                  << num_resolve_obj_ << " " << num_get_migration_dest_ << " "
                  << num_update_location_ << std::endl;
      }
    });
  }
}

ControllerServer::~ControllerServer() {
  done_ = true;
  barrier();
  logging_thread_.Join();
}

std::unique_ptr<RPCRespRegisterNode>
ControllerServer::handle_register_node(const RPCReqRegisterNode &req) {
  if constexpr (kEnableLogging) {
    num_register_node_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespRegisterNode>();
  auto node = req.node;
  auto lpid = req.lpid;
  auto md5 = req.md5;
  auto optional = ctrl_.register_node(node, lpid, md5);
  if (optional) {
    resp->empty = false;
    resp->lpid = optional->first;
    resp->stack_cluster = optional->second;
  } else {
    resp->empty = true;
  }
  return resp;
}

std::unique_ptr<RPCRespVerifyMD5>
ControllerServer::handle_verify_md5(const RPCReqVerifyMD5 &req) {
  if constexpr (kEnableLogging) {
    num_verify_md5_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespVerifyMD5>();
  resp->passed = ctrl_.verify_md5(req.lpid, req.md5);
  return resp;
}

std::unique_ptr<RPCRespAllocateObj>
ControllerServer::handle_allocate_obj(const RPCReqAllocateObj &req) {
  if constexpr (kEnableLogging) {
    num_allocate_obj_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespAllocateObj>();
  auto optional = ctrl_.allocate_obj(req.lpid, req.ip_hint);
  if (optional) {
    resp->empty = false;
    resp->id = optional->first;
    resp->server_ip = optional->second;
  } else {
    resp->empty = true;
  }
  return resp;
}

std::unique_ptr<RPCRespDestroyObj>
ControllerServer::handle_destroy_obj(const RPCReqDestroyObj &req) {
  if constexpr (kEnableLogging) {
    num_destroy_obj_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespDestroyObj>();
  ctrl_.destroy_obj(req.id);
  return resp;
}

std::unique_ptr<RPCRespResolveObj>
ControllerServer::handle_resolve_obj(const RPCReqResolveObj &req) {
  if constexpr (kEnableLogging) {
    num_resolve_obj_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespResolveObj>();
  resp->ip = ctrl_.resolve_obj(req.id);
  return resp;
}

void ControllerServer::handle_update_location(const RPCReqUpdateLocation &req) {
  if constexpr (kEnableLogging) {
    num_update_location_++;
  }

  ctrl_.update_location(req.id, req.obj_srv_ip);
}

std::unique_ptr<RPCRespGetMigrationDest>
ControllerServer::handle_get_migration_dest(const RPCReqGetMigrationDest &req) {
  if constexpr (kEnableLogging) {
    num_get_migration_dest_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespGetMigrationDest>();
  resp->ip = ctrl_.get_migration_dest(req.lpid, req.src_ip, req.resource);
  return resp;
}

std::unique_ptr<RPCRespProbeFreeResource>
ControllerServer::handle_probing(const RPCReqProbeFreeResource &req) {
  auto resp = std::make_unique_for_overwrite<RPCRespProbeFreeResource>();
  resp->resource.cores =
      std::min(rt::RuntimeGlobalIdleCores(),
               rt::RuntimeMaxCores() -
                   (rt::RuntimeActiveCores() - rt::RuntimeSpinningCores())) +
      rt::RuntimeSpinningCores();
  resp->resource.mem_mbs = rt::RuntimeFreeMemMbs();
  return resp;
}

} // namespace nu

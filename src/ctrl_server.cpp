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
      num_update_location_(0), num_report_free_resource_(0), done_(false) {
  if constexpr (kEnableLogging) {
    logging_thread_ = rt::Thread([&] {
      std::cout << "time_us register_node verify_md5 allocate_obj destroy_obj "
                   "resolve_obj get_migration_dest update_location"
                << std::endl;
      while (!rt::access_once(done_)) {
        timer_sleep(kPrintIntervalUs);
        std::cout << microtime() << " " << num_register_node_ << " "
                  << num_verify_md5_ << " " << num_allocate_obj_ << " "
                  << num_destroy_obj_ << " " << num_resolve_obj_ << " "
                  << num_get_migration_dest_ << " " << num_update_location_
                  << std::endl;
      }
    });
  }

  netaddr laddr{.ip = 0, .port = kPort};
  tcp_queue_.reset(rt::TcpQueue::Listen(laddr, kTCPListenBackLog));
  BUG_ON(!tcp_queue_);
  tcp_queue_thread_ = rt::Thread([&] {
    rt::TcpConn *conn;
    while ((conn = tcp_queue_->Accept())) {
      tcp_conns_.emplace_back(conn);
      tcp_conn_threads_.emplace_back([&, conn] { tcp_loop(conn); });
    }
  });
}

ControllerServer::~ControllerServer() {
  done_ = true;
  barrier();
  logging_thread_.Join();

  tcp_queue_.reset();
  barrier();
  tcp_queue_thread_.Join();

  tcp_conns_.clear();
  barrier();
  for (auto &th : tcp_conn_threads_) {
    th.Join();
  }
}

void ControllerServer::tcp_loop(rt::TcpConn *c) {
  RPCReqType rpc_type;
  while (c->ReadFull(&rpc_type, sizeof(rpc_type)) == sizeof(rpc_type)) {
    switch (rpc_type) {
    case kGetMigrationDest: {
      RPCReqGetMigrationDest req;
      ssize_t data_size = sizeof(req) - sizeof(rpc_type);
      BUG_ON(c->ReadFull(&req.rpc_type + 1, data_size) != data_size);
      auto resp = handle_get_migration_dest(req);
      BUG_ON(c->WriteFull(resp.get(), sizeof(*resp)) != sizeof(*resp));
      break;
    }
    case kUpdateLocation: {
      RPCReqUpdateLocation req;
      ssize_t data_size = sizeof(req) - sizeof(rpc_type);
      BUG_ON(c->ReadFull(&req.rpc_type + 1, data_size) != data_size);
      handle_update_location(req);
      break;
    }
    case kReportFreeResource: {
      RPCReqReportFreeResource req;
      ssize_t data_size = sizeof(req) - sizeof(rpc_type);
      BUG_ON(c->ReadFull(&req.rpc_type + 1, data_size) != data_size);
      handle_report_free_resource(req);
      break;
    }
    default:
      BUG();
    }
  }
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

void ControllerServer::handle_report_free_resource(
    const RPCReqReportFreeResource &req) {
  if constexpr (kEnableLogging) {
    num_report_free_resource_++;
  }

  ctrl_.report_free_resource(req.lpid, req.ip, req.resource);
}

} // namespace nu

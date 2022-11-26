#include <algorithm>

extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}
#include <runtime.h>
#include <sync.h>
#include <timer.h>

#include "nu/runtime.hpp"
#include "nu/ctrl_server.hpp"

namespace nu {

ControllerServer::ControllerServer()
    : num_register_node_(0),
      num_allocate_proclet_(0),
      num_destroy_proclet_(0),
      num_resolve_proclet_(0),
      num_acquire_migration_dest_(0),
      num_release_migration_dest_(0),
      num_update_location_(0),
      num_report_free_resource_(0),
      num_get_free_resources_(0),
      done_(false) {
  if constexpr (kEnableLogging) {
    logging_thread_ = rt::Thread([&] {
      std::cout
          << "time_us register_node allocate_proclet destroy_proclet"
             "resolve_proclet acquire_migration_dest release_migration_dest"
             "update_location report_free_resource get_free_resources"
          << std::endl;
      while (!rt::access_once(done_)) {
        timer_sleep(kPrintIntervalUs);
        std::cout << microtime() << " " << num_register_node_ << " "
                  << num_allocate_proclet_ << " " << num_destroy_proclet_ << " "
                  << num_resolve_proclet_ << " " << num_acquire_migration_dest_
                  << " " << num_release_migration_dest_ << " "
                  << num_update_location_ << num_report_free_resource_ << " "
                  << num_get_free_resources_ << std::endl;
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
      case kAcquireMigrationDest: {
        RPCReqAcquireMigrationDest req;
        ssize_t data_size = sizeof(req) - sizeof(rpc_type);
        BUG_ON(c->ReadFull(&req.rpc_type + 1, data_size) != data_size);
        auto resp = handle_acquire_migration_dest(req);
        BUG_ON(c->WriteFull(resp.get(), sizeof(*resp)) != sizeof(*resp));
        break;
      }
      case kReleaseMigrationDest: {
        RPCReqReleaseMigrationDest req;
        ssize_t data_size = sizeof(req) - sizeof(rpc_type);
        BUG_ON(c->ReadFull(&req.rpc_type + 1, data_size) != data_size);
        handle_release_migration_dest(req);
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

std::unique_ptr<RPCRespRegisterNode> ControllerServer::handle_register_node(
    const RPCReqRegisterNode &req) {
  if constexpr (kEnableLogging) {
    num_register_node_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespRegisterNode>();
  auto ip = req.ip;
  auto lpid = req.lpid;
  auto md5 = req.md5;
  auto optional = ctrl_.register_node(ip, lpid, md5);
  if (optional) {
    resp->empty = false;
    resp->lpid = optional->first;
    resp->stack_cluster = optional->second;
  } else {
    resp->empty = true;
  }
  return resp;
}

std::unique_ptr<RPCRespAllocateProclet>
ControllerServer::handle_allocate_proclet(const RPCReqAllocateProclet &req) {
  if constexpr (kEnableLogging) {
    num_allocate_proclet_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespAllocateProclet>();
  auto optional = ctrl_.allocate_proclet(req.capacity, req.lpid, req.ip_hint);
  if (optional) {
    resp->empty = false;
    resp->id = optional->first;
    resp->server_ip = optional->second;
  } else {
    resp->empty = true;
  }
  return resp;
}

void ControllerServer::handle_destroy_proclet(
    const RPCReqDestroyProclet &req) {
  if constexpr (kEnableLogging) {
    num_destroy_proclet_++;
  }

  ctrl_.destroy_proclet(req.heap_segment);
}

std::unique_ptr<RPCRespResolveProclet> ControllerServer::handle_resolve_proclet(
    const RPCReqResolveProclet &req) {
  if constexpr (kEnableLogging) {
    num_resolve_proclet_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespResolveProclet>();
  resp->ip = ctrl_.resolve_proclet(req.id);
  return resp;
}

void ControllerServer::handle_update_location(const RPCReqUpdateLocation &req) {
  if constexpr (kEnableLogging) {
    num_update_location_++;
  }

  ctrl_.update_location(req.id, req.proclet_srv_ip);
}

std::unique_ptr<RPCRespAcquireMigrationDest>
ControllerServer::handle_acquire_migration_dest(
    const RPCReqAcquireMigrationDest &req) {
  if constexpr (kEnableLogging) {
    num_acquire_migration_dest_++;
  }

  auto resp = std::make_unique_for_overwrite<RPCRespAcquireMigrationDest>();
  resp->ip = ctrl_.acquire_migration_dest(req.lpid, req.src_ip, req.resource);
  return resp;
}

void ControllerServer::handle_release_migration_dest(
    const RPCReqReleaseMigrationDest &req) {
  if constexpr (kEnableLogging) {
    num_release_migration_dest_++;
  }

  ctrl_.release_migration_dest(req.lpid, req.ip);
}

void ControllerServer::handle_report_free_resource(
    const RPCReqReportFreeResource &req) {
  if constexpr (kEnableLogging) {
    num_report_free_resource_++;
  }

  ctrl_.report_free_resource(req.lpid, req.ip, req.resource);
}

std::unique_ptr<std::vector<std::pair<NodeIP, Resource>>>
ControllerServer::handle_get_free_resources(const RPCReqGetFreeResources &req) {
  if constexpr (kEnableLogging) {
    num_get_free_resources_++;
  }

  return std::make_unique<std::vector<std::pair<NodeIP, Resource>>>(
      ctrl_.get_free_resources(req.lpid));
}

}  // namespace nu

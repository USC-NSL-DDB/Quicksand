#include <sys/mman.h>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <runtime/thread.h>
}
#include "thread.h"

#include "ctrl_client.hpp"
#include "ctrl_server.hpp"
#include "migrator.hpp"
#include "monitor.hpp"
#include "obj_conn_mgr.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "runtime_deleter.hpp"

namespace nu {

bool active_runtime = false;
ObjServer obj_server;
std::unique_ptr<ControllerServer> controller_server;
std::unique_ptr<Monitor> monitor;

SlabAllocator Runtime::runtime_slab;
RCULock Runtime::rcu_lock;
std::unique_ptr<HeapManager> Runtime::heap_manager;
std::unique_ptr<ControllerClient> Runtime::controller_client;
std::unique_ptr<RemObjConnManager> Runtime::rem_obj_conn_mgr;
std::unique_ptr<Migrator> Runtime::migrator;
std::unique_ptr<ThreadSafeHashMap<
    RemObjID, Runtime::RuntimeFuture<void>,
    RuntimeAllocator<std::pair<const RemObjID, Runtime::RuntimeFuture<void>>>>>
    Runtime::obj_inflight_inc_cnts;

void Runtime::init_runtime_heap() {
  auto addr = reinterpret_cast<void *>(Controller::kMaxVAddr);
  auto mmap_addr = mmap(addr, kRuntimeHeapSize, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != addr);
  BUG_ON(madvise(mmap_addr, kRuntimeHeapSize, MADV_HUGEPAGE) != 0);
  runtime_slab.init(mmap_addr, kRuntimeHeapSize);
}

void Runtime::init_as_controller(netaddr remote_ctrl_addr) {
  controller_server.reset(new ControllerServer(remote_ctrl_addr.port));
  controller_server->run_loop();
}

void Runtime::init_as_server(uint16_t local_obj_srv_port,
                             netaddr remote_ctrl_addr) {
  obj_server.init(local_obj_srv_port);
  rt::Thread obj_srv_thread([&] { obj_server.run_loop(); });
  heap_manager.reset(new decltype(heap_manager)::element_type());
  controller_client.reset(new decltype(controller_client)::element_type(
      local_obj_srv_port, remote_ctrl_addr, true));
  obj_inflight_inc_cnts.reset(
      new decltype(obj_inflight_inc_cnts)::element_type());
  rem_obj_conn_mgr.reset(new decltype(rem_obj_conn_mgr)::element_type());
  migrator.reset(new decltype(migrator)::element_type());
  monitor.reset(new decltype(monitor)::element_type());

  obj_srv_thread.Join();
}

void Runtime::init_as_client(uint16_t local_obj_srv_port,
                             netaddr remote_ctrl_addr) {
  controller_client.reset(new decltype(controller_client)::element_type(
      local_obj_srv_port, remote_ctrl_addr, false));
  obj_inflight_inc_cnts.reset(
      new decltype(obj_inflight_inc_cnts)::element_type());
  rem_obj_conn_mgr.reset(new decltype(rem_obj_conn_mgr)::element_type());
}

Runtime::Runtime(uint16_t local_obj_srv_port, netaddr remote_ctrl_addr,
                 Mode mode) {
  init_runtime_heap();
  active_runtime = true;

  switch (mode) {
  case CONTROLLER:
    init_as_controller(remote_ctrl_addr);
    break;
  case SERVER:
    init_as_server(local_obj_srv_port, remote_ctrl_addr);
    break;
  case CLIENT:
    init_as_client(local_obj_srv_port, remote_ctrl_addr);
    break;
  default:
    BUG();
  }
}

std::unique_ptr<Runtime> Runtime::init(uint16_t local_obj_srv_port,
                                       netaddr remote_ctrl_addr, Mode mode) {
  BUG_ON(active_runtime);
  auto runtime_ptr = new Runtime(local_obj_srv_port, remote_ctrl_addr, mode);
  return std::unique_ptr<Runtime>(runtime_ptr);
}

Runtime::~Runtime() {
  rcu_lock.synchronize();
  controller_client.reset();
  heap_manager.reset();
  rem_obj_conn_mgr.reset();
  monitor.reset();
  migrator.reset();
  obj_inflight_inc_cnts.reset();
  barrier();
  active_runtime = false;
}

} // namespace nu

void *operator new(size_t size) {
  void *ptr;
  auto *slab = reinterpret_cast<nu::SlabAllocator *>(get_uthread_specific());

  if (slab) {
    ptr = slab->allocate(size);
  } else if (nu::active_runtime) {
    ptr = nu::Runtime::runtime_slab.allocate(size);
  } else {
    preempt_disable();
    ptr = malloc(size);
    preempt_enable();
  }
  return ptr;
}

void operator delete(void *ptr) {
  auto *slab = reinterpret_cast<nu::SlabAllocator *>(get_uthread_specific());

  if (slab) {
    slab->free(ptr);
  } else if (nu::active_runtime) {
    nu::Runtime::runtime_slab.free(ptr);
  } else {
    preempt_disable();
    free(ptr);
    preempt_enable();
  }
}

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
std::unique_ptr<ControllerServer> controller_server;

SlabAllocator Runtime::runtime_slab;
RCULock Runtime::rcu_lock;
std::unique_ptr<ObjServer> Runtime::obj_server;
std::unique_ptr<HeapManager> Runtime::heap_manager;
std::unique_ptr<ControllerClient> Runtime::controller_client;
std::unique_ptr<RemObjConnManager> Runtime::rem_obj_conn_mgr;
std::unique_ptr<Migrator> Runtime::migrator;
std::unique_ptr<Monitor> Runtime::monitor;
std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> Runtime::archive_pool;

void Runtime::init_runtime_heap() {
  auto addr = reinterpret_cast<void *>(Controller::kMaxVAddr);
  auto mmap_addr = mmap(addr, kRuntimeHeapSize, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != addr);
  BUG_ON(madvise(mmap_addr, kRuntimeHeapSize, MADV_HUGEPAGE) != 0);
  uint16_t sentinel = 0x1;
  runtime_slab.init(sentinel, mmap_addr, kRuntimeHeapSize);
}

void Runtime::init_as_controller(netaddr ctrl_server_addr) {
  controller_server.reset(new ControllerServer(ctrl_server_addr.port));
  controller_server->run_loop();
}

void Runtime::init_as_server(uint16_t local_obj_srv_port,
                             uint16_t local_migrator_port,
                             netaddr ctrl_server_addr) {
  obj_server.reset(new decltype(obj_server)::element_type(local_obj_srv_port));
  rt::Thread obj_srv_thread([&] { obj_server->run_loop(); });
  migrator.reset(new decltype(migrator)::element_type());
  rt::Thread migrator_thread([&] { migrator->run_loop(local_migrator_port); });
  heap_manager.reset(new decltype(heap_manager)::element_type());
  controller_client.reset(new decltype(controller_client)::element_type(
      local_obj_srv_port, local_migrator_port, ctrl_server_addr));
  rem_obj_conn_mgr.reset(new decltype(rem_obj_conn_mgr)::element_type());
  monitor.reset(new decltype(monitor)::element_type());
  rt::Thread monitor_thread([&] { monitor->run_loop(); });
  archive_pool.reset(new decltype(archive_pool)::element_type());

  obj_srv_thread.Join();
}

void Runtime::init_as_client(netaddr ctrl_server_addr) {
  controller_client.reset(
      new decltype(controller_client)::element_type(ctrl_server_addr));
  rem_obj_conn_mgr.reset(new decltype(rem_obj_conn_mgr)::element_type());
  archive_pool.reset(new decltype(archive_pool)::element_type());
}

Runtime::Runtime(uint16_t local_obj_srv_port, uint16_t local_migrator_port,
                 netaddr ctrl_server_addr, Mode mode) {
  init_runtime_heap();
  active_runtime = true;

  switch (mode) {
  case CONTROLLER:
    init_as_controller(ctrl_server_addr);
    break;
  case SERVER:
    init_as_server(local_obj_srv_port, local_migrator_port, ctrl_server_addr);
    break;
  case CLIENT:
    init_as_client(ctrl_server_addr);
    break;
  default:
    BUG();
  }
}

std::unique_ptr<Runtime> Runtime::init(uint16_t local_obj_srv_port,
                                       uint16_t local_migrator_port,
                                       netaddr ctrl_server_addr, Mode mode) {
  BUG_ON(active_runtime);
  auto runtime_ptr = new Runtime(local_obj_srv_port, local_migrator_port,
                                 ctrl_server_addr, mode);
  return std::unique_ptr<Runtime>(runtime_ptr);
}

Runtime::~Runtime() {
  rcu_lock.writer_sync();
  obj_server.reset();
  controller_client.reset();
  heap_manager.reset();
  rem_obj_conn_mgr.reset();
  monitor.reset();
  migrator.reset();
  archive_pool.reset();
  barrier();
  active_runtime = false;
}

// TODO: make the rcu lock to be per-heap instead of being global.
void Runtime::migration_enable() {
  auto *heap_header = get_obj_heap_header();
  if (!heap_header) {
    return;
  }
  heap_header->threads->put(thread_self());
  heap_manager->rcu_reader_unlock();
}

void Runtime::migration_disable() {
  auto *heap_header = get_obj_heap_header();
  if (!heap_header) {
    return;
  }
  void *heap_base = heap_header;

  heap_manager->rcu_reader_lock();
  if (unlikely(!heap_manager->contains(heap_base))) {
    heap_manager->rcu_reader_unlock();
    while (unlikely(!thread_is_migrated())) {
      thread_yield();
    }
    heap_manager->rcu_reader_lock();
  }
  heap_header->threads->remove(thread_self());
}

void Runtime::reserve_ctrl_server_conns(uint32_t num) {
  if (controller_client) {
    controller_client->reserve_conns(num);
  }
}

void Runtime::reserve_obj_server_conns(uint32_t num, netaddr obj_server_addr) {
  if (rem_obj_conn_mgr) {
    rem_obj_conn_mgr->reserve_conns(num, obj_server_addr);
  }
}

void Runtime::reserve_migration_conns(uint32_t num, netaddr dest_server_addr) {
  if (migrator) {
    migrator->reserve_conns(num, dest_server_addr);
  }
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

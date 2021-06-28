#include <new>
#include <string>
#include <sys/mman.h>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
#include <runtime/thread.h>
}
#include <runtime.h>
#include <thread.h>

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
std::unique_ptr<StackManager> Runtime::stack_manager;
std::unique_ptr<ControllerClient> Runtime::controller_client;
std::unique_ptr<RemObjConnManager> Runtime::rem_obj_conn_mgr;
std::unique_ptr<Migrator> Runtime::migrator;
std::unique_ptr<Monitor> Runtime::monitor;
std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> Runtime::archive_pool;

void Runtime::init_runtime_heap() {
  auto addr = reinterpret_cast<void *>(kMinRuntimeHeapVaddr);
  preempt_disable();
  auto mmap_addr = mmap(addr, kRuntimeHeapSize, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  preempt_enable();
  BUG_ON(mmap_addr != addr);
  BUG_ON(madvise(mmap_addr, kRuntimeHeapSize, MADV_HUGEPAGE) != 0);
  uint16_t sentinel = 0x1;
  runtime_slab.init(sentinel, mmap_addr, kRuntimeHeapSize);
}

void Runtime::init_as_controller() {
  controller_server.reset(new ControllerServer());
  controller_server->run_loop();
}

void Runtime::init_as_server(uint32_t remote_ctrl_ip) {
  obj_server.reset(new decltype(obj_server)::element_type());
  rt::Thread obj_srv_thread([&] { obj_server->run_loop(); });
  migrator.reset(new decltype(migrator)::element_type());
  rt::Thread migrator_thread([&] { migrator->run_loop(); });
  controller_client.reset(
      new decltype(controller_client)::element_type(remote_ctrl_ip, SERVER));
  heap_manager.reset(new decltype(heap_manager)::element_type());
  stack_manager.reset(new decltype(stack_manager)::element_type(
      controller_client->get_stack_cluster()));
  rem_obj_conn_mgr.reset(new decltype(rem_obj_conn_mgr)::element_type());
  monitor.reset(new decltype(monitor)::element_type());
  rt::Thread monitor_thread([&] { monitor->run_loop(); });
  archive_pool.reset(new decltype(archive_pool)::element_type());

  obj_srv_thread.Join();
}

void Runtime::init_as_client(uint32_t remote_ctrl_ip) {
  controller_client.reset(
      new decltype(controller_client)::element_type(remote_ctrl_ip, CLIENT));
  rem_obj_conn_mgr.reset(new decltype(rem_obj_conn_mgr)::element_type());
  archive_pool.reset(new decltype(archive_pool)::element_type());
}

Runtime::Runtime(uint32_t remote_ctrl_ip, Mode mode) {
  init_runtime_heap();
  active_runtime = true;

  switch (mode) {
  case CONTROLLER:
    init_as_controller();
    break;
  case SERVER:
    init_as_server(remote_ctrl_ip);
    break;
  case CLIENT:
    init_as_client(remote_ctrl_ip);
    break;
  default:
    BUG();
  }
}

std::unique_ptr<Runtime> Runtime::init(uint32_t remote_ctrl_ip, Mode mode) {
  BUG_ON(active_runtime);
  auto runtime_ptr = new Runtime(remote_ctrl_ip, mode);
  return std::unique_ptr<Runtime>(runtime_ptr);
}

Runtime::~Runtime() {
  rcu_lock.writer_sync();
  obj_server.reset();
  controller_client.reset();
  heap_manager.reset();
  stack_manager.reset();
  rem_obj_conn_mgr.reset();
  monitor.reset();
  migrator.reset();
  archive_pool.reset();
  barrier();
  active_runtime = false;
  preempt_disable();
  munmap(runtime_slab.get_base(), kRuntimeHeapSize);
  preempt_enable();
}

// TODO: make the rcu lock to be per-heap instead of being global.
void Runtime::migration_enable() {
  auto *heap_header = get_current_obj_heap_header();
  if (!heap_header) {
    return;
  }
  heap_header->threads->put(thread_self());
  heap_manager->rcu_reader_unlock();
}

void Runtime::migration_disable() {
  auto *heap_header = get_current_obj_heap_header();
  if (!heap_header) {
    return;
  }

retry:
  preempt_disable();
  if (unlikely(!heap_manager->rcu_try_reader_lock())) {
    preempt_enable();
    while (unlikely(thread_is_migrating())) {
      rt::Yield();
    }
    goto retry;
  }
  preempt_enable();
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

uint32_t str_to_ip(std::string ip_str) {
  auto pos0 = ip_str.find('.');
  BUG_ON(pos0 == std::string::npos);
  auto pos1 = ip_str.find('.', pos0 + 1);
  BUG_ON(pos1 == std::string::npos);
  auto pos2 = ip_str.find('.', pos1 + 1);
  BUG_ON(pos2 == std::string::npos);
  auto addr0 = stoi(ip_str.substr(0, pos0));
  auto addr1 = stoi(ip_str.substr(pos0 + 1, pos1 - pos0));
  auto addr2 = stoi(ip_str.substr(pos1 + 1, pos2 - pos1));
  auto addr3 = stoi(ip_str.substr(pos2 + 1));
  return MAKE_IP_ADDR(addr0, addr1, addr2, addr3);
}

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func) {
  int ret;
  std::string mode_str;
  Runtime::Mode mode;
  uint32_t remote_ctrl_ip;

  if (argc < 4) {
    goto wrong_args;
  }

  mode_str = std::string(argv[2]);
  if (mode_str == "CLT") {
    mode = nu::Runtime::Mode::CLIENT;
  } else if (mode_str == "SRV") {
    mode = nu::Runtime::Mode::SERVER;
  } else if (mode_str == "CTL") {
    mode = nu::Runtime::Mode::CONTROLLER;
  } else {
    goto wrong_args;
  }

  remote_ctrl_ip = str_to_ip(std::string(argv[3]));

  ret = rt::RuntimeInit(std::string(argv[1]), [&] {
    auto runtime = nu::Runtime::init(remote_ctrl_ip, mode);
    argv[3] = argv[0];
    main_func(argc - 3, &argv[3]);
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: cfg_file CLT/SRV/CTL ctrl_ip [app args] " << std::endl;
  return -EINVAL;
}

} // namespace nu

inline void *__new(size_t size) {
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

void *operator new(size_t size) {
  auto *ptr = __new(size);
  if (ptr) {
    return ptr;
  } else {
    throw std::bad_alloc();
  }
}

void *operator new(size_t size, const std::nothrow_t &nothrow_value) noexcept {
  return __new(size);
}

void operator delete(void *ptr) noexcept {
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

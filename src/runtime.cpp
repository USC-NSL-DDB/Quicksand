#include <new>
#include <string>
#include <sys/mman.h>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
#include <runtime/pressure.h>
#include <runtime/thread.h>
}
#include <runtime.h>
#include <thread.h>

#include "nu/ctrl_client.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/obj_server.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/rpc_client_mgr.hpp"
#include "nu/rpc_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_deleter.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

bool active_runtime = false;

SlabAllocator Runtime::runtime_slab;
RCULock Runtime::rcu_lock;
std::unique_ptr<ObjServer> Runtime::obj_server;
std::unique_ptr<HeapManager> Runtime::heap_manager;
std::unique_ptr<StackManager> Runtime::stack_manager;
std::unique_ptr<ControllerClient> Runtime::controller_client;
std::unique_ptr<ControllerServer> Runtime::controller_server;
std::unique_ptr<RPCClientMgr> Runtime::rpc_client_mgr;
std::unique_ptr<Migrator> Runtime::migrator;
std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> Runtime::archive_pool;
std::unique_ptr<RPCServer> Runtime::rpc_server;
std::unique_ptr<PressureHandler> Runtime::pressure_handler;

void Runtime::init_runtime_heap() {
  auto addr = reinterpret_cast<void *>(kMinRuntimeHeapVaddr);
  preempt_disable();
  auto mmap_addr = mmap(addr, kRuntimeHeapSize, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED, -1, 0);
  preempt_enable();
  BUG_ON(mmap_addr != addr);
  BUG_ON(madvise(mmap_addr, kRuntimeHeapSize, MADV_HUGEPAGE) != 0);
  runtime_slab.init(kRuntimeSlabId, mmap_addr, kRuntimeHeapSize);
}

void Runtime::init_as_controller() {
  controller_server.reset(new decltype(controller_server)::element_type());
  rt::Thread([&] { rpc_server->run_loop(); }).Join();
}

void Runtime::init_as_server(uint32_t remote_ctrl_ip) {
  obj_server.reset(new decltype(obj_server)::element_type());
  rt::Thread([&] { rpc_server->run_loop(); }).Detach();
  migrator.reset(new decltype(migrator)::element_type());
  auto migrator_thread = rt::Thread([&] { migrator->run_loop(); });
  controller_client.reset(
      new decltype(controller_client)::element_type(remote_ctrl_ip, kServer));
  heap_manager.reset(new decltype(heap_manager)::element_type());
  pressure_handler.reset(new decltype(pressure_handler)::element_type());
  stack_manager.reset(new decltype(stack_manager)::element_type(
      controller_client->get_stack_cluster()));
  archive_pool.reset(new decltype(archive_pool)::element_type());
  migrator_thread.Join();
}

void Runtime::init_as_client(uint32_t remote_ctrl_ip) {
  controller_client.reset(
      new decltype(controller_client)::element_type(remote_ctrl_ip, kClient));
  archive_pool.reset(new decltype(archive_pool)::element_type());
}

void Runtime::common_init() {
  init_runtime_heap();
  active_runtime = true;
  rpc_client_mgr.reset(
      new decltype(rpc_client_mgr)::element_type(RPCServer::kPort));
}

Runtime::Runtime(uint32_t remote_ctrl_ip, Mode mode) {
  common_init();

  switch (mode) {
  case kController:
    init_as_controller();
    break;
  case kServer:
    init_as_server(remote_ctrl_ip);
    break;
  case kClient:
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
  rpc_client_mgr.reset();
  migrator.reset();
  archive_pool.reset();
  barrier();
  active_runtime = false;
  preempt_disable();
  munmap(runtime_slab.get_base(), kRuntimeHeapSize);
  preempt_enable();
}

// FIXME
uint32_t Runtime::get_ip_by_rem_obj_id(RemObjID id) {
  auto *owner_heap = thread_unset_owner_heap();
  auto ip = rpc_client_mgr->get_ip_by_rem_obj_id(id);
  thread_set_owner_heap(thread_self(), owner_heap);
  return ip;
}

void Runtime::reserve_conn(uint32_t ip) {
  RuntimeSlabGuard guard;
  rpc_client_mgr->get_by_ip(ip);
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
    mode = nu::Runtime::Mode::kClient;
  } else if (mode_str == "SRV") {
    mode = nu::Runtime::Mode::kServer;
  } else if (mode_str == "CTL") {
    mode = nu::Runtime::Mode::kController;
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
  auto *slab = reinterpret_cast<nu::SlabAllocator *>(thread_get_obj_slab());

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

void *operator new(size_t size) throw() {
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
  if (nu::active_runtime) {
    nu::SlabAllocator::free(ptr);
  } else {
    preempt_disable();
    free(ptr);
    preempt_enable();
  }
}


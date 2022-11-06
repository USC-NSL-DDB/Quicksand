#include <sys/mman.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <new>
#include <string>

extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <net/ip.h>
#include <runtime/thread.h>
}
#include <runtime.h>
#include <thread.h>

#include "nu/command_line.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/proclet_server.hpp"
#include "nu/resource_reporter.hpp"
#include "nu/rpc_client_mgr.hpp"
#include "nu/rpc_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_deleter.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

bool active_runtime = false;

SlabAllocator Runtime::runtime_slab;
std::unique_ptr<ProcletServer> Runtime::proclet_server;
std::unique_ptr<ProcletManager> Runtime::proclet_manager;
std::unique_ptr<StackManager> Runtime::stack_manager;
std::unique_ptr<ControllerClient> Runtime::controller_client;
std::unique_ptr<ControllerServer> Runtime::controller_server;
std::unique_ptr<RPCClientMgr> Runtime::rpc_client_mgr;
std::unique_ptr<Migrator> Runtime::migrator;
std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> Runtime::archive_pool;
std::unique_ptr<RPCServer> Runtime::rpc_server;
std::unique_ptr<PressureHandler> Runtime::pressure_handler;
std::unique_ptr<ResourceReporter> Runtime::resource_reporter;

void Runtime::init_runtime_heap() {
  auto addr = reinterpret_cast<void *>(kMinRuntimeHeapVaddr);
  preempt_disable();
  auto mmap_addr =
      mmap(addr, kRuntimeHeapSize, PROT_READ | PROT_WRITE,
           MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED | MAP_NORESERVE, -1, 0);
  BUG_ON(mmap_addr != addr);
  auto rc = madvise(addr, kRuntimeHeapSize, MADV_DONTDUMP);
  BUG_ON(rc == -1);
  preempt_enable();
  runtime_slab.init(kRuntimeSlabId, mmap_addr, kRuntimeHeapSize,
                    /* aggressive_caching = */ true);
}

void Runtime::init_as_controller() {
  controller_server.reset(new decltype(controller_server)::element_type());
  rpc_server.reset(new decltype(rpc_server)::element_type());
  rpc_server->run_background_loop();
}

void Runtime::init_as_server(uint32_t remote_ctrl_ip, lpid_t lpid) {
  proclet_server.reset(new decltype(proclet_server)::element_type());
  rpc_server.reset(new decltype(rpc_server)::element_type());
  rpc_server->run_background_loop();
  migrator.reset(new decltype(migrator)::element_type());
  migrator->run_background_loop();
  controller_client.reset(new decltype(controller_client)::element_type(
      remote_ctrl_ip, kServer, lpid));
  proclet_manager.reset(new decltype(proclet_manager)::element_type());
  pressure_handler.reset(new decltype(pressure_handler)::element_type());
  resource_reporter.reset(new decltype(resource_reporter)::element_type());
  stack_manager.reset(new decltype(stack_manager)::element_type(
      controller_client->get_stack_cluster()));
  archive_pool.reset(new decltype(archive_pool)::element_type());
}

void Runtime::init_as_client(uint32_t remote_ctrl_ip, lpid_t lpid) {
  controller_client.reset(new decltype(controller_client)::element_type(
      remote_ctrl_ip, kClient, lpid));
  archive_pool.reset(new decltype(archive_pool)::element_type());
}

void Runtime::common_init() {
  prealloc_threads_and_stacks(4 * kNumCores);
  init_runtime_heap();
  active_runtime = true;
  rpc_client_mgr.reset(
      new decltype(rpc_client_mgr)::element_type(RPCServer::kPort));
}

Runtime::Runtime(uint32_t remote_ctrl_ip, Mode mode, lpid_t lpid) {
  common_init();

  if (mode == kClient) {
    init_as_client(remote_ctrl_ip, lpid);
  } else {
    if (mode == kController) {
      init_as_controller();
    } else if (mode == kServer) {
      init_as_server(remote_ctrl_ip, lpid);
    } else {
      BUG();
    }

    rt::Preempt p;
    rt::PreemptGuardAndPark pg(&p);
  }
}

std::unique_ptr<Runtime> Runtime::init(uint32_t remote_ctrl_ip, Mode mode,
                                       lpid_t lpid) {
  BUG_ON(active_runtime);
  auto runtime_ptr = new Runtime(remote_ctrl_ip, mode, lpid);
  return std::unique_ptr<Runtime>(runtime_ptr);
}

Runtime::~Runtime() {
  proclet_server.reset();
  controller_client.reset();
  proclet_manager.reset();
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

void Runtime::reserve_conns(uint32_t ip) {
  RuntimeSlabGuard guard;
  migrator->reserve_conns(ip);
  rpc_client_mgr->get_by_ip(ip);
}

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func) {

  AllOptionsDesc all_options_desc;
  all_options_desc.parse(argc, argv);

  auto mode = all_options_desc.vm.count("server") ? nu::Runtime::Mode::kServer
                                                  : nu::Runtime::Mode::kClient;
  auto ctrl_ip = str_to_ip(all_options_desc.nu.ctrl_ip_str);
  auto lpid = all_options_desc.nu.lpid;
  auto conf_path = all_options_desc.caladan.conf_path;
  if (conf_path.empty()) {
    conf_path = ".conf_" + std::to_string(getpid());
    write_options_to_file(conf_path, all_options_desc);
  }

  auto ret = rt::RuntimeInit(conf_path, [&] {
    if (conf_path.starts_with(".conf_")) {
      BUG_ON(remove(conf_path.c_str()));
    }
    auto runtime = nu::Runtime::init(ctrl_ip, mode, lpid);
    for (int i = 0; i < argc; i++) {
      if (strcmp(argv[i], "--") == 0 || i == argc - 1) {
        argc -= i;
        argv[i] = argv[0];
        argv += i;
        break;
      }
    }
    main_func(argc, argv);
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

}  // namespace nu

inline void *__new(size_t size) {
  void *ptr;
  auto *slab = reinterpret_cast<nu::SlabAllocator *>(thread_get_proclet_slab());

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
  BUG_ON(size && !ptr);
  return ptr;
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

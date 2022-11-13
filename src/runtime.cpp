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

Runtime::Runtime() {}

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

    caladan_->thread_park();
  }
}

Runtime::~Runtime() {
  proclet_server_.reset();
  controller_client_.reset();
  proclet_manager_.reset();
  stack_manager_.reset();
  rpc_client_mgr_.reset();
  migrator_.reset();
  archive_pool_.reset();
  caladan_.reset();
  runtime_slab_.reset();
}

void Runtime::init_runtime_heap() {
  auto addr = reinterpret_cast<void *>(kMinRuntimeHeapVaddr);
  {
    Caladan::PreemptGuard g;

    auto mmap_addr =
        mmap(addr, kRuntimeHeapSize, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_SHARED | MAP_FIXED | MAP_NORESERVE, -1, 0);
    BUG_ON(mmap_addr != addr);
    auto rc = madvise(addr, kRuntimeHeapSize, MADV_DONTDUMP);
    BUG_ON(rc == -1);
  }
  runtime_slab_.reset(new decltype(runtime_slab_)::element_type(
      kRuntimeSlabId, addr, kRuntimeHeapSize,
      /* aggressive_caching = */ true));
}

void Runtime::init_as_controller() {
  controller_server_.reset(new decltype(controller_server_)::element_type());
  rpc_server_.reset(new decltype(rpc_server_)::element_type());
  rpc_server_->run_background_loop();
}

void Runtime::init_as_server(uint32_t remote_ctrl_ip, lpid_t lpid) {
  proclet_server_.reset(new decltype(proclet_server_)::element_type());
  rpc_server_.reset(new decltype(rpc_server_)::element_type());
  rpc_server_->run_background_loop();
  migrator_.reset(new decltype(migrator_)::element_type());
  migrator_->run_background_loop();
  controller_client_.reset(new decltype(controller_client_)::element_type(
      remote_ctrl_ip, kServer, lpid));
  proclet_manager_.reset(new decltype(proclet_manager_)::element_type());
  pressure_handler_.reset(new decltype(pressure_handler_)::element_type());
  resource_reporter_.reset(new decltype(resource_reporter_)::element_type());
  stack_manager_.reset(new decltype(stack_manager_)::element_type(
      controller_client_->get_stack_cluster()));
  archive_pool_.reset(new decltype(archive_pool_)::element_type());
}

void Runtime::init_as_client(uint32_t remote_ctrl_ip, lpid_t lpid) {
  controller_client_.reset(new decltype(controller_client_)::element_type(
      remote_ctrl_ip, kClient, lpid));
  archive_pool_.reset(new decltype(archive_pool_)::element_type());
}

void Runtime::common_init() {
  prealloc_threads_and_stacks(4 * kNumCores);
  init_runtime_heap();
  caladan_.reset(new decltype(caladan_)::element_type());
  rpc_client_mgr_.reset(
      new decltype(rpc_client_mgr_)::element_type(RPCServer::kPort));
}

void Runtime::reserve_conns(uint32_t ip) {
  RuntimeSlabGuard guard;
  migrator_->reserve_conns(ip);
  rpc_client_mgr_->get_by_ip(ip);
}

void Runtime::send_rpc_resp_ok(ArchivePool<>::OASStream *oa_sstream,
                               ArchivePool<>::IASStream *ia_sstream,
                               RPCReturner *returner) {
  auto view = oa_sstream->ss.view();
  auto data = reinterpret_cast<const std::byte *>(view.data());
  auto len = oa_sstream->ss.tellp();

  if (likely(!caladan_->thread_has_been_migrated())) {
    auto span = std::span(data, len);

    returner->Return(kOk, span, [this, oa_sstream]() {
      archive_pool_->put_oa_sstream(oa_sstream);
    });
  } else {
    migrator_->forward_to_original_server(kOk, returner, len, data, ia_sstream);
    archive_pool_->put_oa_sstream(oa_sstream);
  }
}

void Runtime::send_rpc_resp_wrong_client(RPCReturner *returner) {
  BUG_ON(caladan_->thread_has_been_migrated());
  returner->Return(kErrWrongClient);
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
    for (int i = 0; i < argc; i++) {
      if (strcmp(argv[i], "--") == 0 || i == argc - 1) {
        argc -= i;
        argv[i] = argv[0];
        argv += i;
        break;
      }
    }
    new (get_runtime()) Runtime(ctrl_ip, mode, lpid);
    main_func(argc, argv);
    std::destroy_at(get_runtime());
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

}  // namespace nu

inline void *__new(size_t size) {
  nu::Caladan::PreemptGuard g;
  void *ptr;
  auto *slab = nu::Caladan::thread_self()
                   ? nu::get_runtime()->caladan()->thread_get_proclet_slab()
                   : nullptr;
  if (slab) {
    ptr = slab->allocate(size);
  } else if (auto *runtime_slab = nu::get_runtime()->runtime_slab()) {
    ptr = runtime_slab->allocate(size);
  } else {
    ptr = malloc(size);
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
  nu::Caladan::PreemptGuard g;

  if (nu::get_runtime()->runtime_slab()) {
    nu::SlabAllocator::free(ptr);
  } else {
    free(ptr);
  }
}

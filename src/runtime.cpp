#include <openssl/crypto.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
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
#include "nu/proclet.hpp"
#include "nu/proclet_server.hpp"
#include "nu/resource_reporter.hpp"
#include "nu/rpc_client_mgr.hpp"
#include "nu/rpc_server.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/slab.hpp"

#ifdef DDB_SUPPORT
#include <ddb/integration.hpp>
#include "ddb_helper/aux_thread.hpp"
#endif

namespace nu {

Runtime::Runtime() {}

Runtime::Runtime(uint32_t remote_ctrl_ip, Mode mode, lpid_t lpid,
                 bool isol) {
  init_base();

  if (mode == kMainServer) {
    init_as_server(remote_ctrl_ip, lpid, /* main = */ true, isol);
  } else {
    if (mode == kController) {
      init_as_controller();
    } else if (mode == kServer) {
      init_as_server(remote_ctrl_ip, lpid, /* main = */ false, isol);
    } else {
      BUG();
    }

    caladan_->thread_park();
  }
}

Runtime::~Runtime() {
  destroy();
  destroy_base();
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
  runtime_slab_ = new SlabAllocator(kRuntimeSlabId, addr, kRuntimeHeapSize,
                                    /* aggressive_caching = */ true);
}

void Runtime::init_as_controller() {
  controller_server_ = new ControllerServer();
}

void Runtime::init_as_server(uint32_t remote_ctrl_ip, lpid_t lpid, bool main,
                             bool isol) {
  proclet_server_ = new ProcletServer();
  migrator_ = new Migrator();
  controller_client_ = new ControllerClient(remote_ctrl_ip, lpid, main, isol);
  proclet_manager_ = new ProcletManager();
  stack_manager_ = new StackManager(controller_client_->get_stack_cluster());
  archive_pool_ = new ArchivePool<>();
  pressure_handler_ = new PressureHandler();
  resource_reporter_ = new ResourceReporter();
}

void Runtime::init_base() {
  prealloc_threads_and_stacks(4 * kNumCores);
  init_runtime_heap();
  caladan_ = new Caladan();
  rpc_client_mgr_ = new RPCClientMgr(RPCServer::kPort);
  rpc_server_ = new RPCServer();
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

void Runtime::shutdown(RPCReturner *returner) {
  destroy();
  returner->Return(kOk);
  rt::Spawn([&] {
    destroy_base();
    exit(0);
  });
}

void Runtime::destroy() {
  delete resource_reporter_;
  delete pressure_handler_;
  delete stack_manager_;
  delete proclet_manager_;
  delete migrator_;
  delete proclet_server_;
  delete controller_client_;
  delete archive_pool_;
  delete controller_server_;
}

void Runtime::destroy_base() {
  delete rpc_client_mgr_;
  delete rpc_server_;
  delete caladan_;
  OPENSSL_cleanup();
  delete runtime_slab_;
  store_release(&runtime_slab_, nullptr);
}

namespace {

void setup_main_proclet(Runtime *runtime) {
  auto *main_header = reinterpret_cast<ProcletHeader *>(kMainProcletHeapVAddr);
  runtime->proclet_manager()->setup(main_header, kMainProcletHeapSize,
                                    /* migratable = */ false,
                                    /* from_migration = */ false);
  main_header->status() = kPresent;
  runtime->caladan()->thread_set_owner_proclet(Caladan::thread_self(),
                                               main_header, false);
}

}  // namespace

#ifdef DDB_SUPPORT
void spawn_ddb_aux_thread() {
  if (!DDB::start_ddb_aux_thread()) {
    std::cerr << "Failed to start DDB auxiliary thread" << std::endl;
  }
}
#endif

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func) {
  AllOptionsDesc all_options_desc;
  all_options_desc.parse(argc, argv);

  auto mode = all_options_desc.vm.count("main") ? nu::Runtime::Mode::kMainServer
                                                : nu::Runtime::Mode::kServer;
  auto ctrl_ip = str_to_ip(all_options_desc.nu.ctrl_ip_str);
  auto lpid = all_options_desc.nu.lpid;
  auto conf_path = all_options_desc.caladan.conf_path;
  auto isol = all_options_desc.vm.count("isol");
  if (conf_path.empty()) {
    conf_path = ".conf_" + std::to_string(getpid());
    write_options_to_file(conf_path, all_options_desc);
  }
  
  
#ifdef DDB_SUPPORT
  auto enable_ddb = all_options_desc.vm.count("ddb");
  auto ddb_addr = all_options_desc.nu.ddb_addr;
  if (enable_ddb) {
    auto ddb_config = DDB::Config::get_default(ddb_addr);
    auto caladan_ip = all_options_desc.caladan.ip;
    std::map<std::string, std::string> user_data = {
      {"caladan_ip", caladan_ip},
      // {"lpid", std::to_string(lpid)},
    };
    ddb_config.with_user_data(user_data);
    auto connector = DDB::DDBConnector(ddb_config);
    connector.init();
  }
  
  spawn_ddb_aux_thread();
#endif

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
    auto *runtime = get_runtime_nocheck();
    new (runtime) Runtime(ctrl_ip, mode, lpid, isol);
    setup_main_proclet(runtime);
    main_func(argc, argv);
    get_runtime()->controller_client()->destroy_lp();
    std::destroy_at(get_runtime());
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

}  // namespace nu

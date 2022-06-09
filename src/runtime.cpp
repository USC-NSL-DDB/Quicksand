#include <sys/mman.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <cstring>
#include <exception>
#include <fstream>
#include <new>
#include <string>

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
RCULock Runtime::rcu_lock;
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
  runtime_slab.init(kRuntimeSlabId, mmap_addr, kRuntimeHeapSize);
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
  rcu_lock.writer_sync();
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

// FIXME
uint32_t Runtime::get_ip_by_proclet_id(ProcletID id) {
  RuntimeSlabGuard g;
  auto *owner_proclet = thread_unset_owner_proclet();
  auto ip = rpc_client_mgr->get_ip_by_proclet_id(id);
  thread_set_owner_proclet(thread_self(), owner_proclet);
  return ip;
}

void Runtime::reserve_conns(uint32_t ip) {
  RuntimeSlabGuard guard;
  migrator->reserve_conns(ip);
  rpc_client_mgr->get_by_ip(ip);
}

void either_options(const boost::program_options::variables_map &vm,
                    const char *opt1, const char *opt2) {
  if (vm.count(opt1) && !vm[opt1].defaulted() && vm.count(opt2) &&
      !vm[opt2].defaulted()) {
    throw std::logic_error(std::string("Both '") + opt1 + "' and '" + opt2 +
                           "' are specified.");
  }
  if (!vm.count(opt1) && !vm[opt1].defaulted() && !vm.count(opt2) &&
      !vm[opt2].defaulted()) {
    throw std::logic_error(std::string("Neither '") + opt1 + "' nor '" + opt2 +
                           "' is specified.");
  }
}

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func) {
  auto default_length =
      boost::program_options::options_description::m_default_line_length;
  boost::program_options::options_description all_desc(
      "Usage: nu_args caladan_args [--] [app_args]", default_length * 2,
      default_length);
  boost::program_options::options_description nu_desc(
      "Nu arguments", default_length * 2, default_length);
  boost::program_options::options_description caladan_desc(
      "Caladan arguments", default_length * 2, default_length);

  Runtime::Mode mode;
  lpid_t lpid;
  std::string ctrl_ip_str;
  uint32_t ctrl_ip;
  int kthreads, guaranteed, spinning;
  std::string conf_path, ip, netmask, gateway;

  nu_desc.add_options()
    ("help,h", "print help")
    ("server,s", "proclet server mode")
    ("client,c", "client mode")
    ("controller,t", boost::program_options::value(&ctrl_ip_str)->default_value("18.18.1.1"), "controller ip")
    ("lpid,l", boost::program_options::value(&lpid)->required(), "logical process id")
    ("memps", "react to memory pressure (only useful for server)")
    ("cpups", "react to CPU pressure (only useful for server)");

  caladan_desc.add_options()
    ("conf,f", boost::program_options::value(&conf_path), "caladan configuration file")
    ("kthreads,k", boost::program_options::value(&kthreads)->default_value(kNumCores - 2), "number of kthreads (if conf unspecified)")
    ("guaranteed,g", boost::program_options::value(&guaranteed)->default_value(0), "number of guaranteed kthreads (if conf unspecified)")
    ("spinning,p", boost::program_options::value(&spinning)->default_value(0), "number of spinning kthreads (if conf unspecified)")
    ("ip,i", boost::program_options::value(&ip), "IP address used in caladan (if conf unspecified)")
    ("netmask,m", boost::program_options::value(&netmask)->default_value("255.255.255.0"), "netmask used in caladan (if conf unspecified)")
    ("gateway,w", boost::program_options::value(&gateway)->default_value("18.18.1.1"), "gateway used in caladan (if conf unspecified)");

  all_desc.add(nu_desc).add(caladan_desc);

  try {
    boost::program_options::variables_map vm;
    boost::program_options::store(parse_command_line(argc, argv, all_desc), vm);
    boost::program_options::notify(vm);

    either_options(vm, "conf", "kthreads");
    either_options(vm, "conf", "guaranteed");
    either_options(vm, "conf", "spinning");
    either_options(vm, "conf", "ip");
    either_options(vm, "conf", "netmask");
    either_options(vm, "conf", "gateway");
    either_options(vm, "server", "client");

    if (vm.count("help")) {
      std::cout << all_desc << std::endl;
      return 0;
    }

    if (vm.count("server")) {
      mode = nu::Runtime::Mode::kServer;
    } else {
      mode = nu::Runtime::Mode::kClient;
    }

    ctrl_ip = str_to_ip(ctrl_ip_str);

    if (conf_path.empty()) {
      conf_path = std::string(".conf_") + std::to_string(getpid());
      std::ofstream ofs(conf_path);
      ofs << "host_addr " << ip << std::endl;
      ofs << "host_netmask " << netmask << std::endl;
      ofs << "host_gateway " << gateway << std::endl;
      ofs << "host_mtu " << 9000 << std::endl;
      ofs << "runtime_kthreads " << kthreads << std::endl;
      ofs << "runtime_guaranteed_kthreads " << guaranteed << std::endl;
      ofs << "runtime_spinning_kthreads " << spinning << std::endl;
      ofs << "runtime_qdelay_us " << 0 << std::endl;
      ofs << "enable_directpath " << 1 << std::endl;
      ofs << "log_level " << 0 << std::endl;
      if (mode == nu::Runtime::Mode::kServer) {
        if (vm.count("memps")) {
          ofs << "runtime_react_mem_pressure 1" << std::endl;
        }
        if (vm.count("cpups")) {
          ofs << "runtime_react_cpu_pressure 1" << std::endl;
        }
      }
    }
  } catch (std::exception &e) {
    std::cout << all_desc << std::endl;
    std::cerr << e.what() << std::endl;
    return -EINVAL;
  }
  auto ret = rt::RuntimeInit(conf_path, [&] {
    auto runtime = nu::Runtime::init(ctrl_ip, mode, lpid);
    for (uint32_t i = 0; i < argc; i++) {
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
  BUG_ON(!ptr);
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

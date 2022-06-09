#include <boost/program_options.hpp>
#include <fstream>

extern "C" {
#include <runtime/net.h>
}
#include <runtime.h>

#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/runtime.hpp"

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

int main(int argc, char **argv) {
  auto default_length =
      boost::program_options::options_description::m_default_line_length;
  boost::program_options::options_description desc("Usage:", default_length * 2,
                                                   default_length);

  int kthreads, guaranteed, spinning;
  std::string conf_path, ip, netmask, gateway;

  desc.add_options()
    ("help,h", "print help")
    ("conf,f", boost::program_options::value(&conf_path), "caladan configuration file")
    ("kthreads,k", boost::program_options::value(&kthreads)->default_value(nu::kNumCores - 2), "number of kthreads (if conf unspecified)")
    ("guaranteed,g", boost::program_options::value(&guaranteed)->default_value(0), "number of guaranteed kthreads (if conf unspecified)")
    ("spinning,p", boost::program_options::value(&spinning)->default_value(0), "number of spinning kthreads (if conf unspecified)")
    ("ip,i", boost::program_options::value(&ip)->default_value("18.18.1.1"), "IP address used in caladan (if conf unspecified)")
    ("netmask,m", boost::program_options::value(&netmask)->default_value("255.255.255.0"), "netmask used in caladan (if conf unspecified)")
    ("gateway,w", boost::program_options::value(&gateway)->default_value("18.18.1.1"), "gateway used in caladan (if conf unspecified)");

  try {
    boost::program_options::variables_map vm;
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    either_options(vm, "conf", "kthreads");
    either_options(vm, "conf", "guaranteed");
    either_options(vm, "conf", "spinning");
    either_options(vm, "conf", "ip");
    either_options(vm, "conf", "netmask");
    either_options(vm, "conf", "gateway");

    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }
  } catch (std::exception &e) {
    std::cout << desc << std::endl;
    std::cerr << e.what() << std::endl;
    return -EINVAL;
  }

  if (conf_path.empty()) {
    conf_path = std::string(".conf_") + std::to_string(getpid());
    std::ofstream ofs(conf_path);
    ofs << "host_addr " << ip << std::endl;
    ofs << "host_netmask " << netmask << std::endl;
    ofs << "host_gateway " << gateway << std::endl;
    ofs << "runtime_kthreads " << kthreads << std::endl;
    ofs << "runtime_guaranteed_kthreads " << guaranteed << std::endl;
    ofs << "runtime_spinning_kthreads " << spinning << std::endl;
    ofs << "runtime_qdelay_us " << 0 << std::endl;
    ofs << "enable_directpath " << 1 << std::endl;
    ofs << "log_level " << 0 << std::endl;
  }

  auto ret = rt::RuntimeInit(conf_path, [&] {
    nu::Runtime::init(get_cfg_ip(), nu::Runtime::Mode::kController, 0);
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

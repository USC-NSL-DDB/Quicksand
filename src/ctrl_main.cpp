#include <unistd.h>

#include <cstdio>
#include <fstream>

extern "C" {
#include <runtime/net.h>
}
#include <runtime.h>

#include "nu/command_line.hpp"
#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/runtime.hpp"

constexpr auto kIP = "18.18.1.1";

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
  nu::CaladanOptionsDesc desc(kIP);
  desc.parse(argc, argv);

  auto conf_path = desc.conf_path;
  if (conf_path.empty()) {
    conf_path = ".conf_" + std::to_string(getpid());
    write_options_to_file(conf_path, desc);
  }

  auto ret = rt::RuntimeInit(conf_path, [&] {
    if (conf_path.starts_with(".conf_")) {
      BUG_ON(remove(conf_path.c_str()));
    }
    nu::Runtime::init(get_cfg_ip(), nu::Runtime::Mode::kController, 0);
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

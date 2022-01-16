extern "C" {
#include <runtime/net.h>
}

#include <runtime.h>

#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/runtime.hpp"

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    goto wrong_args;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [&] {
    nu::Runtime::init(get_cfg_ip(), nu::Runtime::Mode::kController, 0);
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: cfg_file" << std::endl;
  return -EINVAL;
}

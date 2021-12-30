#include <runtime.h>

#include "nu/commons.hpp"
#include "nu/ctrl_server.hpp"
#include "nu/runtime.hpp"

int main(int argc, char **argv) {
  int ret;
  uint32_t ctrl_ip;

  if (argc < 3) {
    goto wrong_args;
  }

  ctrl_ip = nu::str_to_ip(std::string(argv[2]));

  ret = rt::RuntimeInit(std::string(argv[1]), [&] {
    nu::Runtime::init(ctrl_ip, nu::Runtime::Mode::kController);
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: cfg_file ctrl_ip" << std::endl;
  return -EINVAL;
}

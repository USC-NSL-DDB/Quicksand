#include <chrono>

#include <thread.h>
#include <runtime.h>

#include "dataloader.hpp"

using namespace std::chrono;
using namespace imagenet;

std::string datapath = "/opt/kaiyan/imagenet/train_t3";
constexpr auto kNumThreads = 1000;

void do_work() {
  auto start = high_resolution_clock::now();
  auto dataloader = BaselineDataLoader(datapath, 1, kNumThreads);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  start = high_resolution_clock::now();
  dataloader.process_all();
  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image pre-processing takes " << duration.count() << "ms"
            << std::endl;
}

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(argv[1], [] { do_work(); });
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

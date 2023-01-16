#include <chrono>

#include "dataloader.hpp"

using namespace std::chrono;
using namespace imagenet;

std::string datapath = "/opt/kaiyan/imagenet/train_t3";
constexpr auto batch_size = 1;

void do_work() {
  auto imgs = nu::make_sharded_vector<RawImage, std::false_type>();

  auto start = high_resolution_clock::now();
  auto dataloader = DataLoader(datapath, batch_size);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "DataLoader: Image loading takes " << duration.count() << "ms" << std::endl;

  start = high_resolution_clock::now();
  dataloader.process_all();
  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "DataLoader: Image pre-processing takes " << duration.count() << "ms"
            << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

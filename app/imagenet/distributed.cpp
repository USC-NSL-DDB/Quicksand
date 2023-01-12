#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>

#include "image_kernel.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using directory_iterator = std::filesystem::recursive_directory_iterator;
using shard_type = nu::ShardedVector<imagenet::Image, std::false_type>;
using sealed_shard_type = nu::SealedDS<shard_type>;
using namespace std::chrono;
using namespace imagenet;

class Worker {
 public:
  Worker(shard_type imgs) : imgs_(std::move(imgs)) {}

  void run() {
    auto sealed_imgs = nu::to_sealed_ds(std::move(imgs_));
    for (const auto &img : sealed_imgs) {
      kernel(img);
      {
        rt::Preempt p;
        rt::PreemptGuard g(&p);
        nu::get_runtime()->pressure_handler()->mock_set_pressure();
      }
    }
  }

 private:
  shard_type imgs_;
};

void load(std::string path, shard_type &imgs) {
  int i = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();
      Image image(fname);
      imgs.push_back(image);
      i++;
    }
  }
  std::cout << i << " images loaded" << std::endl;
}

void do_work() {
  auto imgs = nu::make_sharded_vector<Image, std::false_type>();
  std::string datapath = "train_t3";

  auto start = high_resolution_clock::now();
  load(datapath, imgs);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  auto worker =
      nu::make_proclet<Worker>(std::tuple(std::move(imgs)));
  start = high_resolution_clock::now();
  worker.run(&Worker::run);
  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image pre-processing takes " << duration.count() << "ms"
            << std::endl;
  cv::cleanup();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

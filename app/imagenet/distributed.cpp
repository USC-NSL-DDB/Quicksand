#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>

#include <nu/pressure_handler.hpp>
#include <nu/runtime.hpp>
#include <nu/sealed_ds.hpp>
#include <nu/sharded_vector.hpp>
#include <nu/sharded_vector.hpp>
#include <nu/dis_executor.hpp>

#include "image_kernel.hpp"

using directory_iterator = std::filesystem::recursive_directory_iterator;
using shard_type = nu::ShardedVector<imagenet::Image, std::false_type>;
using sealed_shard_type = nu::SealedDS<shard_type>;
using namespace std::chrono;
using namespace imagenet;

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
  std::string datapath = "/opt/kaiyan/imagenet/train_t3";

  auto start = high_resolution_clock::now();
  load(datapath, imgs);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  auto sealed_imgs = nu::to_sealed_ds(std::move(imgs));
  auto imgs_range = nu::make_contiguous_ds_range(sealed_imgs);

  start = high_resolution_clock::now();
  auto dis_exec = nu::make_distributed_executor(
      +[](decltype(imgs_range) &imgs_range) {
        while (!imgs_range.empty()) {
          auto img = imgs_range.pop();
          kernel(img);
        }
      },
      imgs_range);
  dis_exec.get();
  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image pre-processing takes " << duration.count() << "ms"
            << std::endl;
  cv::cleanup();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

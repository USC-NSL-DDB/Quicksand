#include <filesystem>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <iterator>

#include "image_kernel.hpp"

#include "nu/runtime.hpp"
#include "nu/sharded_vector.hpp"

using directory_iterator = std::filesystem::recursive_directory_iterator;
using shard_type = nu::ShardedVector<imagenet::Image, std::integral_constant<bool, false>>;
using namespace std::chrono;
using namespace imagenet;

void load(std::string path, shard_type &imgs)
{
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

void process(int thread_id, int thread_cnt, shard_type &imgs)
{
  for (size_t i = thread_id; i < imgs.size(); i += thread_cnt) {
    kernel(imgs[i]);
  }
}

void do_work() {
  auto imgs = nu::make_sharded_vector<Image, std::false_type>();
  std::string datapath = "/opt/kaiyan/imagenet/train_t3";

  auto start = high_resolution_clock::now();
  load(datapath, imgs);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  int thread_cnt = 4;
  std::vector<std::thread> threads;

  std::cout << imgs.size() << std::endl;

  start = high_resolution_clock::now();

  for (int i = 0; i < thread_cnt; i++) {
    threads.push_back(std::thread(process, i, thread_cnt, std::ref(imgs)));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image pre-processing takes " << duration.count() << "ms" << std::endl;
}

int main(int argc, char **argv)
{
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
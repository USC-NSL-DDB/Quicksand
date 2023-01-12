#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <opencv2/opencv.hpp>
#include <string>

#include <thread.h>
#include <runtime.h>

#include "image_kernel.hpp"

using directory_iterator = std::filesystem::recursive_directory_iterator;
using namespace std::chrono;
using namespace imagenet;

constexpr auto kNumThreads = 1000;

std::string datapath = "train_t3";
std::vector<Image> imgs;

void load(std::string path) {
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

void process(int tid) {
  auto num_imgs_per_thread = (imgs.size() - 1) / kNumThreads + 1;
  auto start_idx = num_imgs_per_thread * tid;
  auto end_idx = std::min(imgs.size(), start_idx + num_imgs_per_thread);

  for (size_t i = start_idx; i < end_idx; i++) {
    kernel(imgs[i]);
  }
}

void do_work() {
  auto start = high_resolution_clock::now();
  load(datapath);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  start = high_resolution_clock::now();

  std::vector<rt::Thread> threads;
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([tid = i] { process(tid); });
  }
  for (auto &thread : threads) {
    thread.Join();
  }

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

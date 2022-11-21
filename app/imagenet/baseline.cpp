#include <filesystem>
#include <opencv2/opencv.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <iterator>

#include "image_kernel.hpp"

using directory_iterator = std::filesystem::recursive_directory_iterator;
using namespace std::chrono;
using namespace imagenet;

std::string datapath = "/opt/kaiyan/imagenet/train_t3";

std::vector<Image> imgs;

void load(std::string path)
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

void process(int thread_id, int thread_cnt)
{
  for (size_t i = thread_id; i < imgs.size(); i += thread_cnt) {
    kernel(imgs[i]);
  }
}

int main(int argc, char **argv)
{
  auto start = high_resolution_clock::now();
  load(datapath);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  int thread_cnt = 4;
  std::vector<std::thread> threads;

  start = high_resolution_clock::now();

  for (int i = 0; i < thread_cnt; i++) {
    threads.push_back(std::thread(process, i, thread_cnt));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image pre-processing takes " << duration.count() << "ms" << std::endl;

  return 0;
}
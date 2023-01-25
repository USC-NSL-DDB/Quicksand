#include "dataloader.hpp"

#include <runtime.h>
#include <thread.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <string>

using directory_iterator = std::filesystem::recursive_directory_iterator;
using namespace std::chrono;
using namespace imagenet;

DataLoader::DataLoader(std::string path, int batch_size) {
  batch_size_ = batch_size;
  progress_ = 0;
  imgs_ = nu::make_sharded_vector<Image, std::false_type>();

  int i = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();
      Image image(fname);
      imgs_.push_back(image);
      i++;
    }
  }
  std::cout << "DataLoader: " << i << " images loaded" << std::endl;
}

std::size_t DataLoader::size() const { return imgs_.size(); }

void DataLoader::process_all() {
  auto sealed_imgs = nu::to_sealed_ds(std::move(imgs_));
  auto imgs_range = nu::make_contiguous_ds_range(sealed_imgs);

  auto dis_exec = nu::make_distributed_executor(
      +[](decltype(imgs_range) &imgs_range) {
        while (true) {
          auto img = imgs_range.pop();
          if (!img) {
            break;
          }
          kernel(std::move(*img));
        }
      },
      imgs_range);
  dis_exec.get();

  imgs_ = nu::to_unsealed_ds(std::move(sealed_imgs));
  // shardedVector.size() self blocks
  // progress_ = imgs_.size();
  cv::cleanup();
}

Batch DataLoader::next() { return Batch(); }

BaselineDataLoader::BaselineDataLoader(std::string path, int batch_size,
                                       int nthreads) {
  batch_size_ = batch_size;
  nthreads_ = nthreads;
  progress_ = 0;

  int i = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();
      Image image(fname);
      imgs_.push_back(image);
      i++;
    }
  }
  std::cout << "BaselineDataLoader: " << i << " images loaded" << std::endl;
}

void BaselineDataLoader::process(int tid) {
  auto num_imgs_per_thread = (imgs_.size() - 1) / nthreads_ + 1;
  auto start_idx = num_imgs_per_thread * tid;
  auto end_idx = std::min(imgs_.size(), start_idx + num_imgs_per_thread);

  for (size_t i = start_idx; i < end_idx; i++) {
    kernel(imgs_[i]);
  }
}

void BaselineDataLoader::process_all() {
  std::vector<rt::Thread> threads;
  for (int i = 0; i < nthreads_; i++) {
    threads.emplace_back([tid = i, this] { process(tid); });
  }
  for (auto &thread : threads) {
    thread.Join();
  }

  progress_ = imgs_.size();
}

Batch BaselineDataLoader::next() { return Batch(); }

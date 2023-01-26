#include <runtime.h>
#include <thread.h>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <string>

#include "gpu.hpp"
#include "dataloader.hpp"
#include "baseline_gpu.hpp"

using directory_iterator = std::filesystem::recursive_directory_iterator;
using namespace std::chrono;
using namespace imagenet;

DataLoader::DataLoader(std::string path)
    : imgs_{nu::make_sharded_vector<RawImage, std::false_type>()},
      queue_{nu::make_sharded_queue<Image, std::true_type>()} {
  int image_count = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();
      RawImage image(fname);
      imgs_.push_back(image);
      image_count++;
    }
  }
  std::cout << "DataLoader: " << image_count << " images loaded" << std::endl;
}

std::size_t DataLoader::size() const { return imgs_.size(); }

DataLoader::~DataLoader() { cv::cleanup(); }

void DataLoader::process_all() {
  using Elem = Image;
  using GPU = MockGPU<Elem>;

  auto sealed_imgs = nu::to_sealed_ds(std::move(imgs_));
  auto imgs_range = nu::make_contiguous_ds_range(sealed_imgs);

  auto queue = nu::make_sharded_queue<Elem, std::true_type>();

  auto gpus = std::vector<nu::Proclet<GPU>>{};
  auto futures = std::vector<nu::Future<void>>{};
  for (uint64_t i = 0; i < kMaxNumGPUs; ++i) {
    gpus.emplace_back(nu::make_proclet<GPU>(true, std::nullopt, kGPUIP));
    futures.emplace_back(gpus.back().run_async(&GPU::run, queue));
  }

  auto producers = nu::make_distributed_executor(
      +[](decltype(imgs_range) &imgs_range, decltype(queue) queue) {
        while (true) {
          auto img = imgs_range.pop();
          if (!img) {
            break;
          }
          auto processed = kernel(std::move(*img));
          queue.push(std::move(processed));
        }
      },
      imgs_range, queue);

  producers.get();
  for (auto &gpu : gpus) {
    gpu.run(&GPU::drain_and_stop);
  }
  for (auto &f : futures) {
    f.get();
  }

  imgs_ = nu::to_unsealed_ds(std::move(sealed_imgs));

  cv::cleanup();
}

BaselineDataLoader::BaselineDataLoader(std::string path, int nthreads)
    : nthreads_(nthreads) {
  int i = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();
      RawImage image(fname);
      imgs_.push_back(image);
      i++;
    }
  }
  std::cout << "BaselineDataLoader: " << i << " images loaded" << std::endl;
}

rt::TcpConn *BaselineDataLoader::dial_gpu_server() {
  netaddr laddr{.ip = 0, .port = 0};
  netaddr raddr{.ip = kBaselineGPUIP, .port = kBaselineGPUPort};
  auto *conn = rt::TcpConn::Dial(laddr, raddr);
  BUG_ON(!conn);
  return conn;
}

void BaselineDataLoader::process(int tid) {
  auto *conn = dial_gpu_server();

  auto num_imgs_per_thread = (imgs_.size() - 1) / nthreads_ + 1;
  auto start_idx = num_imgs_per_thread * tid;
  auto end_idx = std::min(imgs_.size(), start_idx + num_imgs_per_thread);

  for (size_t i = start_idx; i < end_idx; i++) {
    auto processed = kernel(imgs_[i]);
    auto &data = processed.data;
    uint64_t size = data.size();
    const iovec iovecs[] = {{&size, sizeof(size)}, {data.data(), size}};
    BUG_ON(conn->WritevFull(std::span(iovecs)) < 0);
    bool ack;
    BUG_ON(conn->ReadFull(&ack, sizeof(ack)) <= 0);
  }
}

void BaselineDataLoader::process_all() {
  auto *main_conn = dial_gpu_server();

  std::vector<rt::Thread> threads;
  for (int i = 0; i < nthreads_; i++) {
    threads.emplace_back([tid = i, this] { process(tid); });
  }
  for (auto &thread : threads) {
    thread.Join();
  }

  bool exit;
  BUG_ON(main_conn->WriteFull(&exit, sizeof(exit)) < 0);
  bool ack;
  BUG_ON(main_conn->ReadFull(&ack, sizeof(ack)) <= 0);
}

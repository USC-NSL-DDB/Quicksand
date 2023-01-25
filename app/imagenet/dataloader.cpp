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

template <uint32_t Micros>
void compute_us() {
  const auto num_iters = Micros * cycles_per_us;
  for (uint32_t i = 0; i < num_iters; i++) {
    asm volatile("nop");
  }
}

enum GPUStatus {
  kRunning = 0,
  kDrain,
  kTerminate,
};

using GPUStatusType = uint8_t;

template <typename Item>
class MockGPU {
 public:
  static constexpr uint32_t kProcessDelayUs = 3500;

  MockGPU() {}
  void run(nu::ShardedQueue<Item, std::true_type> queue) {
    constexpr std::size_t kPopNumItems = 10;

    while (true) {
      auto status = rt::access_once(status_);
      if (unlikely(status == GPUStatus::kTerminate)) {
        break;
      }
      auto popped = queue.try_pop(kPopNumItems);
      if (unlikely(status == GPUStatus::kDrain && popped.size() == 0)) {
        break;
      }
      for (auto &p : popped) {
        process(std::move(p));
      }
    }
  }
  void drain_and_stop() { status_ = GPUStatus::kDrain; }
  void stop() { status_ = GPUStatus::kTerminate; }

 private:
  void process(Item &&item) { compute_us<kProcessDelayUs>(); }

  GPUStatusType status_ = GPUStatus::kRunning;
};

DataLoader::DataLoader(std::string path, int batch_size)
    : imgs_{nu::make_sharded_vector<RawImage, std::false_type>()},
      queue_{nu::make_sharded_queue<Image, std::true_type>()},
      batch_size_{batch_size},
      progress_{0} {
  int i = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();
      RawImage image(fname);
      imgs_.push_back(image);
      i++;
    }
  }
  std::cout << "DataLoader: " << i << " images loaded" << std::endl;
}

std::size_t DataLoader::size() const { return imgs_.size(); }

DataLoader::~DataLoader() { cv::cleanup(); }

void DataLoader::process_all() {
  using Elem = Image;
  using GPU = MockGPU<Elem>;

  constexpr uint64_t kNumGPUs = 40;

  auto sealed_imgs = nu::to_sealed_ds(std::move(imgs_));
  auto imgs_range = nu::make_contiguous_ds_range(sealed_imgs);

  auto queue = nu::make_sharded_queue<Elem, std::true_type>();

  auto gpus = std::vector<nu::Proclet<GPU>>{};
  auto futures = std::vector<nu::Future<void>>{};
  for (uint64_t i = 0; i < kNumGPUs; ++i) {
    gpus.emplace_back(nu::make_proclet<GPU>());
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

Image DataLoader::next() {
  // auto consumer = nu::make_proclet<shard_queue_type>();
  // auto img = consumer.run(shard_queue_type::consume);
  return Image();
}

BaselineDataLoader::BaselineDataLoader(std::string path, int batch_size,
                                       int nthreads) {
  batch_size_ = batch_size;
  nthreads_ = nthreads;
  progress_ = 0;

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

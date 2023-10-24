#pragma once

#include <fstream>
#include <nu/dis_executor.hpp>
#include <nu/pressure_handler.hpp>
#include <nu/runtime.hpp>
#include <nu/sealed_ds.hpp>
#include <nu/sharded_queue.hpp>
#include <nu/sharded_vector.hpp>
#include <opencv2/opencv.hpp>

#include "gpu.hpp"
#include "image.hpp"

namespace imagenet {

using Batch = std::vector<RawImage>;
using shard_vec_type = nu::ShardedVector<RawImage, std::false_type>;
using sealed_shard_vec_type = nu::SealedDS<shard_vec_type>;
using shard_queue_type = nu::ShardedQueue<Image, std::true_type>;

class DataLoader {
 public:
  using GPU = MockGPU<Image>;
  using GPUTraces = typename GPU::Traces;

  static constexpr auto kGPUIP = MAKE_IP_ADDR(18, 18, 1, 100);
  static constexpr uint32_t kNumScaleUpGPUs = 10;
  static constexpr uint64_t kNumScaleDownGPUs = 5;
  static constexpr uint64_t kScaleUpDurationUs = nu::kOneMilliSecond * 500;
  static constexpr uint64_t kScaleDownDurationUs = nu::kOneMilliSecond * 500;

  DataLoader(std::string path);
  std::size_t size() const;
  ~DataLoader();
  uint64_t process_all();

 private:
  GPUTraces run_gpus();
  void process_traces(GPUTraces &&all_traces);

  shard_vec_type imgs_;
  shard_queue_type queue_;
  bool processed_ = false;
};

class BaselineDataLoader {
 public:
  constexpr static int kGPUIP = MAKE_IP_ADDR(18, 18, 1, 100);
  constexpr static int kGPUPort = 8888;
  constexpr static int kNumGPUs = 10;

  BaselineDataLoader(std::string path, int nthreads);
  void process_all();

 private:
  void process(int tid);
  rt::TcpConn* dial_gpu_server();

  int nthreads_;
  std::vector<RawImage> imgs_;
};

}  // namespace imagenet

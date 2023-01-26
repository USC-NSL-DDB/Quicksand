#pragma once

#include <fstream>
#include <nu/dis_executor.hpp>
#include <nu/pressure_handler.hpp>
#include <nu/runtime.hpp>
#include <nu/sealed_ds.hpp>
#include <nu/sharded_queue.hpp>
#include <nu/sharded_vector.hpp>
#include <opencv2/opencv.hpp>

#include "image.hpp"

namespace imagenet {

using Batch = std::vector<RawImage>;
using shard_vec_type = nu::ShardedVector<RawImage, std::false_type>;
using sealed_shard_vec_type = nu::SealedDS<shard_vec_type>;
using shard_queue_type = nu::ShardedQueue<Image, std::true_type>;

class DataLoader {
 public:
  static constexpr uint32_t kMaxNumGPUs = 46;
  static constexpr auto kGPUIP = MAKE_IP_ADDR(18, 18, 1, 10);

  DataLoader(std::string path);
  std::size_t size() const;
  ~DataLoader();
  void process_all();

 private:
  shard_vec_type imgs_;
  shard_queue_type queue_;
};

class BaselineDataLoader {
 public:
  BaselineDataLoader(std::string path, int nthreads);
  void process_all();

 private:
  void process(int tid);
  rt::TcpConn *dial_gpu_server();

  int nthreads_;
  std::vector<RawImage> imgs_;
};

}  // namespace imagenet

#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

#include <nu/pressure_handler.hpp>
#include <nu/runtime.hpp>
#include <nu/sealed_ds.hpp>
#include <nu/sharded_vector.hpp>
#include <nu/sharded_queue.hpp>
#include <nu/dis_executor.hpp>

#include "image.hpp"

namespace imagenet {

using Batch = std::vector<RawImage>;
using shard_vec_type = nu::ShardedVector<RawImage, std::false_type>;
using sealed_shard_vec_type = nu::SealedDS<shard_vec_type>;
// using shard_queue_type = nu::ShardedQueue<cv::Mat, std::true_type>;

class DataLoader {
 public:
  DataLoader(std::string path, int batch_size);
  ~DataLoader();
  // preprocess all
  void process_all();
  // kickstart the preprocess but doesn't wait for them to finish
  void process();
  Image next();

 private:
  shard_vec_type imgs_;
  // shard_queue_type queue_;
  int batch_size_, progress_;
};

class BaselineDataLoader {
 public:
  BaselineDataLoader(std::string path, int batch_size, int nthreads_);
  void process_all();
  Batch next();

 private:
  void process(int tid);

  std::vector<RawImage> imgs_;
  int batch_size_, nthreads_, progress_;
};

}

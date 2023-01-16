#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

#include <nu/pressure_handler.hpp>
#include <nu/runtime.hpp>
#include <nu/sealed_ds.hpp>
#include <nu/sharded_vector.hpp>
#include <nu/dis_executor.hpp>

#include "image.hpp"

namespace imagenet {

using Batch = std::vector<Image>;
using shard_type = nu::ShardedVector<imagenet::Image, std::false_type>;
using sealed_shard_type = nu::SealedDS<shard_type>;

class DataLoader {
 public:
  DataLoader(std::string path, int batch_size);
  void process_all();
  Batch next();

 private:
  shard_type imgs_;
  int batch_size_, progress_;
};

class BaselineDataLoader {
 public:
  BaselineDataLoader(std::string path, int batch_size, int nthreads_);
  void process_all();
  Batch next();

 private:
  void process(int tid);

  std::vector<Image> imgs_;
  int batch_size_, nthreads_, progress_;
};

}

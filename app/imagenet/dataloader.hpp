#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

#include "image_kernel.hpp"

namespace imagenet {

using Batch = std::vector<Image>;

// class DataLoader {
//  public:
//   DataLoader(std::string path, size_t batch_size);
//   void process_all();
//   Image next();

//  private:
//   std::vector<Image> imgs_;
//   size_t batch_size_;
// };

class BaselineDataLoader {
 public:
  BaselineDataLoader(std::string path, size_t batch_size, size_t nthreads_);
  void process_all();
  Batch next();

 private:
  void process(int tid);

  std::vector<Image> imgs_;
  size_t batch_size_, nthreads_, progress_;
}

}

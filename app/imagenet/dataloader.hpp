#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

#include "image.hpp"

namespace imagenet {

using Batch = std::vector<Image>;

// class DataLoader {
//  public:
//   DataLoader(std::string path, int batch_size);
//   void process_all();
//   Image next();

//  private:
//   std::vector<Image> imgs_;
//   int batch_size_;
// };

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

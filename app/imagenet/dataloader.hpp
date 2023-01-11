#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

#include "image_kernel.hpp"

namespace imagenet {

using Batch = std::vector<Image>;

class DataLoader {
 public:
  DataLoader(std::string path, size_t batch_size);
  Image next();

 private:
  size_t batch_size;
};

class BaselineDataLoader {
 public:
  BaselineDataLoader(std::string path, size_t batch_size, size_t nthread);
  Image next();

 private:
  size_t batch_size;
}

}

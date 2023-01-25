#pragma once

#include <fstream>
#include <opencv2/opencv.hpp>

namespace imagenet {

struct Image {
  Image();
  Image(std::string path);
  std::vector<char> data;

  template <class Archive>
  void save(Archive &ar) const {
    ar(data);
  }

  template <class Archive>
  void load(Archive &ar) {
    ar(data);
  }
};

cv::Mat kernel(Image image);

}  // namespace imagenet

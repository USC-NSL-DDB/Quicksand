#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

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

}

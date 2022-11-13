#pragma once

#include <opencv2/opencv.hpp>
#include <fstream>

namespace imagenet {

struct Image {
  Image(std::string path);
  std::vector<char> data;
};

cv::Mat kernel(Image image);

}

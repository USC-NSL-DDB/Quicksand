#include <iostream>

#include "image.hpp"

namespace imagenet {

Image::Image() {
  // empty image
}

Image::Image(std::string path) {
  std::ifstream file(path, std::ios::binary);

  file.seekg(0, std::ios::end);
  size_t fileSize = file.tellg();
  file.seekg(0, std::ios::beg);

  data.resize(fileSize);
  file.read(data.data(), fileSize);
}

cv::Mat kernel(Image image) {
  cv::Mat raw_img(1, image.data.size(), CV_8UC1, image.data.data());
  cv::Mat img = cv::imdecode(raw_img, cv::IMREAD_COLOR);
  if (img.data == NULL) {
    std::cout << "Decode image error!" << std::endl;
    exit(-1);
  }

  cv::Rect roi(0, 0, std::min(img.cols, 224), std::min(img.rows, 224));
  img = img(roi);
  cv::flip(img, img, 0);

  cv::Point2f pc(img.cols/2., img.rows/2.);
  cv::Mat r = cv::getRotationMatrix2D(pc, 20, 1.0);
  cv::warpAffine(img, img, r, img.size());

  cv::normalize(img, img, 0, 1, cv::NORM_MINMAX, CV_32F);

  // cloning the cropped image allows opencv to free unused memory
  cv::Mat ret = img.clone();
  return ret;
}

}

#include <iostream>

#include "image_kernel.hpp"

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
  cv::Mat resize_img = img(roi);
  cv::Mat flipped_img;
  cv::flip(resize_img, flipped_img, 0);

  cv::Mat rotated_img;
  cv::Point2f pc(flipped_img.cols/2., flipped_img.rows/2.);
  cv::Mat r = cv::getRotationMatrix2D(pc, 20, 1.0);
  cv::warpAffine(flipped_img, rotated_img, r, flipped_img.size());

  cv::Mat norm_img;
  cv::normalize(rotated_img, norm_img, 0, 1, cv::NORM_MINMAX, CV_32F);

  return norm_img;
}

}

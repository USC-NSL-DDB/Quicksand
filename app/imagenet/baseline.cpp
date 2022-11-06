#include <filesystem>
#include <opencv2/opencv.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <iterator>

using directory_iterator = std::filesystem::recursive_directory_iterator;
using namespace std::chrono;

typedef struct img {
  std::vector<char> data;
} img_t;

std::vector<img> imgs;

void load(std::string path)
{
  int i = 0;
  for (const auto &file_ : directory_iterator(path)) {
    if (file_.is_regular_file()) {
      const auto fname = file_.path().string();

      std::ifstream file(fname, std::ios::binary);
      file.unsetf(std::ios::skipws);
      std::streampos fileSize;

      file.seekg(0, std::ios::end);
      fileSize = file.tellg();
      file.seekg(0, std::ios::beg);

      img_t img;
      img.data.reserve(fileSize);
      img.data.insert(img.data.begin(),
                      std::istream_iterator<char>(file),
                      std::istream_iterator<char>());
      imgs.push_back(img);

      i++;
    }
  }
  std::cout << i << " images loaded" << std::endl;
}

cv::Mat kernel(img_t &byte_img)
{
  cv::Mat raw_img(1, byte_img.data.size(), CV_8UC1, byte_img.data.data());
  cv::Mat img = cv::imdecode(raw_img, cv::IMREAD_COLOR);
  if (img.data == NULL) {
    std::cout << "Decode image error!" << std::endl;
    exit(-1);
  }

  cv::Rect roi(0, 0, std::min(img.cols, 224), std::min(img.rows, 224));
  cv::Mat resize_img = img(roi);
  cv::Mat flipped_img;
  cv::flip(resize_img, flipped_img, 0);

  // cv::Mat rotated_img;
  // cv::Point2f pc(flipped_img.cols/2., flipped_img.rows/2.);
  // cv::Mat r = cv::getRotationMatrix2D(pc, 20, 1.0);
  // cv::warpAffine(flipped_img, rotated_img, r, flipped_img.size());

  cv::Mat norm_img;
  cv::normalize(flipped_img, norm_img, 0, 1, cv::NORM_MINMAX, CV_32F);

  return norm_img;
}

void process(int thread_id, int thread_cnt)
{
  for (int i = thread_id; i < imgs.size(); i += thread_cnt) {
    kernel(imgs[i]);
  }
}

int main(int argc, char **argv)
{
  std::string datapath = "/opt/kaiyan/imagenet/train_t3";

  auto start = high_resolution_clock::now();
  load(datapath);
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image loading takes " << duration.count() << "ms" << std::endl;

  int thread_cnt = 4;
  std::vector<std::thread> threads;

  start = high_resolution_clock::now();

  int thread_size = imgs.size() / thread_cnt;
  for (int i = 0; i < thread_cnt; i++) {
    threads.push_back(std::thread(process, i, thread_cnt));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  end = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(end - start);
  std::cout << "Image pre-processing takes " << duration.count() << "ms" << std::endl;

  return 0;
}
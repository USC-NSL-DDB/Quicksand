#pragma once

#include <chrono>
#include <map>
#include <random>

#include <nu/mutex.hpp>

#include "../gen-cpp/UrlShortenService.h"
#include "../gen-cpp/social_network_types.h"

#define HOSTNAME "http://short-url/"

namespace social_network {

class UrlShortenService {
public:
  UrlShortenService();
  std::vector<Url> ComposeUrls(int64_t, std::vector<std::string>);
  std::vector<std::string> GetExtendedUrls(int64_t,
                                           std::vector<std::string> &&);

private:
  std::mt19937 _generator;
  std::uniform_int_distribution<int> _distribution;
  nu::Mutex _thread_lock;
  // TODO: use DistributedHashTable.
  std::map<std::string, std::string> _short_to_extended_map;

  std::string GenRandomStr(int length);
};

UrlShortenService::UrlShortenService()
    : _generator(
          std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() %
                       0xffffffff)) {
  _distribution = std::uniform_int_distribution<int>(0, 61);
}

std::string UrlShortenService::GenRandomStr(int length) {
  const char char_map[] = "abcdefghijklmnopqrstuvwxyzABCDEF"
                          "GHIJKLMNOPQRSTUVWXYZ0123456789";
  std::string return_str;
  _thread_lock.Lock();
  for (int i = 0; i < length; ++i) {
    return_str.append(1, char_map[_distribution(_generator)]);
  }
  _thread_lock.Unlock();
  return return_str;
}

std::vector<Url>
UrlShortenService::ComposeUrls(int64_t req_id,
                               std::vector<std::string> urls) {
  std::vector<Url> target_urls;

  if (!urls.empty()) {
    for (auto &url : urls) {
      Url new_target_url;
      new_target_url.expanded_url = url;
      new_target_url.shortened_url = HOSTNAME +
          GenRandomStr(10);
      target_urls.push_back(new_target_url);
    }

    for (auto &target_url : target_urls) {
      _short_to_extended_map[target_url.shortened_url] =
          target_url.expanded_url;
    }
  }
  return target_urls;
}

std::vector<std::string>
UrlShortenService::GetExtendedUrls(int64_t req_id,
                                   std::vector<std::string> &&shortened_urls) {
  std::vector<std::string> extended_urls;
  for (auto &shortened_url : shortened_urls) {
    extended_urls.push_back(_short_to_extended_map[shortened_url]);
  }
  return extended_urls;
}

} // namespace social_network

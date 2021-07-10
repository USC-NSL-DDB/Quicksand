#pragma once

#include <chrono>
#include <nu/dis_hash_table.hpp>
#include <nu/mutex.hpp>
#include <random>

#include "../gen-cpp/social_network_types.h"

#define HOSTNAME "http://short-url/"

namespace social_network {

class UrlShortenService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;

  UrlShortenService();
  std::vector<Url> ComposeUrls(std::vector<std::string>);
  std::vector<std::string> GetExtendedUrls(std::vector<std::string> &&);

private:
  std::mt19937 _generator;
  std::uniform_int_distribution<int> _distribution;
  nu::Mutex _thread_lock;
  nu::DistributedHashTable<std::string, std::string> _short_to_extended_map;

  std::string GenRandomStr(int length);
};

UrlShortenService::UrlShortenService()
    : _generator(
          std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() %
                       0xffffffff)),
      _distribution(std::uniform_int_distribution<int>(0, 61)),
      _short_to_extended_map(kDefaultHashTablePowerNumShards) {}

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

std::vector<Url> UrlShortenService::ComposeUrls(std::vector<std::string> urls) {
  std::vector<Url> target_urls;

  if (!urls.empty()) {
    for (auto &url : urls) {
      Url new_target_url;
      new_target_url.expanded_url = url;
      new_target_url.shortened_url = HOSTNAME +
          GenRandomStr(10);
      target_urls.push_back(new_target_url);
    }

    std::vector<nu::Future<void>> put_futures;
    for (auto &target_url : target_urls) {
      put_futures.emplace_back(_short_to_extended_map.put_async(
          target_url.shortened_url, target_url.expanded_url));
    }
    for (auto &put_future : put_futures) {
      put_future.get();
    }
  }
  return target_urls;
}

std::vector<std::string>
UrlShortenService::GetExtendedUrls(std::vector<std::string> &&shortened_urls) {
  std::vector<std::string> extended_urls;
  for (auto &shortened_url : shortened_urls) {
    auto extended_urls_optional =
        _short_to_extended_map.get(std::move(shortened_url));
    BUG_ON(!extended_urls_optional);
    extended_urls.emplace_back(std::move(*extended_urls_optional));
  }
  return extended_urls;
}

} // namespace social_network

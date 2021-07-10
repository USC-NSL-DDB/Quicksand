#pragma once

#include <nu/dis_hash_table.hpp>
#include <string>

namespace social_network {

class MediaStorageService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;

  MediaStorageService();
  void UploadMedia(std::string &&filename, std::string &&data);
  std::string GetMedia(std::string &&filename);

private:
  nu::DistributedHashTable<std::string, std::string> _filename_to_data_map;
};

MediaStorageService::MediaStorageService()
    : _filename_to_data_map(kDefaultHashTablePowerNumShards) {}

void MediaStorageService::UploadMedia(std::string &&filename,
                                      std::string &&data) {
  _filename_to_data_map.put(std::move(filename), std::move(data));
}

std::string MediaStorageService::GetMedia(std::string &&filename) {
  auto optional = _filename_to_data_map.get(std::move(filename));
  BUG_ON(!optional);
  return *optional;
}

} // namespace social_network

#pragma once

#include <chrono>
#include <iostream>
#include <string>

// 2018-01-01 00:00:00 UTC
#define CUSTOM_EPOCH 1514764800000

namespace social_network {

class MediaService {
public:
  std::vector<Media> ComposeMedia(std::vector<std::string> &&media_types,
                                  std::vector<int64_t> &&media_ids);

private:
};

std::vector<Media>
MediaService::ComposeMedia(std::vector<std::string> &&media_types,
                           std::vector<int64_t> &&media_ids) {
  std::vector<Media> ret;
  if (media_types.size() != media_ids.size()) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message =
        "The lengths of media_id list and media_type list are not equal";
    throw se;
  }

  for (int i = 0; i < media_ids.size(); ++i) {
    Media new_media;
    new_media.media_id = media_ids[i];
    new_media.media_type = media_types[i];
    ret.emplace_back(new_media);
  }
  return ret;
}

} // namespace social_network

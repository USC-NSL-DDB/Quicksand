#pragma once

#include <iostream>
#include <nu/dis_hash_table.hpp>

#include "../gen-cpp/social_network_types.h"

namespace social_network {

class PostStorageService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;

  PostStorageService();
  void StorePost(Post post);
  Post ReadPost(int64_t post_id);
  std::vector<Post> ReadPosts(std::vector<int64_t> post_ids);

private:
  nu::DistributedHashTable<int64_t, Post> _postid_to_post_map;
};

PostStorageService::PostStorageService()
    : _postid_to_post_map(kDefaultHashTablePowerNumShards) {}

void PostStorageService::StorePost(social_network::Post post) {
  _postid_to_post_map.put(post.post_id, std::move(post));
}

Post PostStorageService::ReadPost(int64_t post_id) {
  auto optional = _postid_to_post_map.get(post_id);
  BUG_ON(!optional);
  return *optional;
}

std::vector<Post> PostStorageService::ReadPosts(std::vector<int64_t> post_ids) {
  std::vector<nu::Future<std::optional<Post>>> post_futures;
  for (auto post_id : post_ids) {
    post_futures.emplace_back(_postid_to_post_map.get_async(post_id));
  }
  std::vector<Post> posts;
  for (auto &post_future : post_futures) {
    auto optional = post_future.get();
    if (optional) {
      posts.emplace_back(*optional);
    }
  }
  return posts;
}

}  // namespace social_network

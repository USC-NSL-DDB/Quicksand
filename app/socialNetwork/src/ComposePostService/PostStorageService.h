#pragma once

#include <map>
#include <iostream>

#include "../../gen-cpp/social_network_types.h"

namespace social_network {

class PostStorageService {
public:
  void StorePost(int64_t req_id, Post &&post);
  Post ReadPost(int64_t req_id, int64_t post_id);
  std::vector<Post> ReadPosts(int64_t req_id, std::vector<int64_t> &&post_ids);

private:
  // TODO: replace it with DistributedHashTable.
  std::map<int64_t, Post> _postid_to_post_map;
};

void PostStorageService::StorePost(int64_t req_id,
                                   social_network::Post &&post) {
  _postid_to_post_map[post.post_id] = post;
}

Post PostStorageService::ReadPost(int64_t req_id, int64_t post_id) {
  return _postid_to_post_map[post_id];
}

std::vector<Post>
PostStorageService::ReadPosts(int64_t req_id, std::vector<int64_t> &&post_ids) {
  std::vector<Post> posts;
  for (auto post_id : post_ids) {
    posts.push_back(_postid_to_post_map[post_id]);
  }
  return posts;
}

}  // namespace social_network


#pragma once

#include <iostream>
#include <nu/rem_obj.hpp>
#include <string>

#include "PostStorageService.h"
#include "SocialGraphService.h"

namespace social_network {

class HomeTimelineService {
public:
  HomeTimelineService(
      nu::RemObj<PostStorageService>::Cap post_storage_service_obj_cap,
      nu::RemObj<SocialGraphService>::Cap social_graph_service_obj_cap);
  std::vector<Post> ReadHomeTimeline(int64_t, int, int);
  void WriteHomeTimeline(int64_t, int64_t, int64_t, std::vector<int64_t> &&);

private:
  nu::RemObj<PostStorageService> _post_storage_service_obj;
  nu::RemObj<SocialGraphService> _social_graph_service_obj;
  // TODO: use DistributedHashTable.
  std::map<int64_t, std::set<std::pair<int64_t, int64_t>>>
      _userid_to_timeline_map;
};

HomeTimelineService::HomeTimelineService(
    nu::RemObj<PostStorageService>::Cap post_storage_service_obj_cap,
    nu::RemObj<SocialGraphService>::Cap social_graph_service_obj_cap)
    : _post_storage_service_obj(post_storage_service_obj_cap),
      _social_graph_service_obj(social_graph_service_obj_cap) {}

void HomeTimelineService::WriteHomeTimeline(
    int64_t post_id, int64_t user_id, int64_t timestamp,
    std::vector<int64_t> &&user_mentions_id) {
  auto ids =
      _social_graph_service_obj.run(&SocialGraphService::GetFollowers, user_id);
  ids.insert(ids.end(), user_mentions_id.begin(), user_mentions_id.end());
  for (auto id : ids) {
    // TODO: need synchronization.
    _userid_to_timeline_map[id].emplace(timestamp, post_id);
  }
}

std::vector<Post> HomeTimelineService::ReadHomeTimeline(int64_t user_id,
                                                        int start, int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  // TODO: need synchronization.
  std::vector<int64_t> post_ids;
  auto &timeline_set = _userid_to_timeline_map[user_id];
  // TODO: use a better data structure to reduce the time complexity from
  // O(nlogn) into O(n).
  auto start_iter = timeline_set.rbegin();
  std::advance(start_iter, start);
  auto stop_iter = timeline_set.rbegin();
  std::advance(stop_iter,
               std::min(static_cast<int>(timeline_set.size()), stop));
  for (auto iter = start_iter; iter != stop_iter; iter++) {
    post_ids.push_back(iter->second);
  }
  return _post_storage_service_obj.run(&PostStorageService::ReadPosts,
                                       std::move(post_ids));
}

} // namespace social_network

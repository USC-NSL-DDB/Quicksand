#pragma once

#include <algorithm>
#include <future>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <nu/dis_hash_table.hpp>
#include <nu/rem_obj.hpp>
#include <set>
#include <string>
#include <utility>

namespace social_network {

class UserTimelineService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;

  UserTimelineService(nu::RemObj<PostStorageService>::Cap cap);
  void WriteUserTimeline(int64_t post_id, int64_t user_id, int64_t timestamp);
  std::vector<Post> ReadUserTimeline(int64_t, int, int);

private:
  nu::RemObj<PostStorageService> _post_storage_service_obj;
  nu::DistributedHashTable<int64_t, std::set<std::pair<int64_t, int64_t>>>
      _userid_to_timeline_map;
};

UserTimelineService::UserTimelineService(
    nu::RemObj<PostStorageService>::Cap cap)
    : _post_storage_service_obj(cap),
      _userid_to_timeline_map(kDefaultHashTablePowerNumShards) {}

void UserTimelineService::WriteUserTimeline(int64_t post_id, int64_t user_id,
                                            int64_t timestamp) {
  // TODO: need synchronization.
  // TODO: computation offloading.
  auto timeline_set_optional = _userid_to_timeline_map.get(user_id);
  auto timeline_set = timeline_set_optional
                          ? std::move(*timeline_set_optional)
                          : std::set<std::pair<int64_t, int64_t>>();
  timeline_set.emplace(timestamp, post_id);
  _userid_to_timeline_map.put(user_id, timeline_set);
}

std::vector<Post> UserTimelineService::ReadUserTimeline(int64_t user_id,
                                                        int start, int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  // TODO: need synchronization.
  // TODO: computation offloading.
  std::vector<int64_t> post_ids;
  auto timeline_set_optional = _userid_to_timeline_map.get(user_id);
  auto timeline_set = timeline_set_optional
                          ? std::move(*timeline_set_optional)
                          : std::set<std::pair<int64_t, int64_t>>();
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

}  // namespace social_network

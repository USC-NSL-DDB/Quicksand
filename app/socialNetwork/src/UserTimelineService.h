#pragma once

#include <algorithm>
#include <cereal/types/pbds_tree.hpp>
#include <ext/pb_ds/assoc_container.hpp>
#include <future>
#include <iostream>
#include <iterator>
#include <limits>
#include <nu/dis_hash_table.hpp>
#include <nu/rem_obj.hpp>
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
  using Tree = __gnu_pbds::tree<int64_t, int64_t, std::greater<int64_t>,
                                __gnu_pbds::rb_tree_tag,
                                __gnu_pbds::tree_order_statistics_node_update>;
  nu::RemObj<PostStorageService> _post_storage_service_obj;
  nu::DistributedHashTable<int64_t, Tree> _userid_to_timeline_map;
};

UserTimelineService::UserTimelineService(
    nu::RemObj<PostStorageService>::Cap cap)
    : _post_storage_service_obj(cap),
      _userid_to_timeline_map(kDefaultHashTablePowerNumShards) {}

void UserTimelineService::WriteUserTimeline(int64_t post_id, int64_t user_id,
                                            int64_t timestamp) {
  _userid_to_timeline_map.apply(
      user_id,
      +[](std::pair<const int64_t, Tree> &p, int64_t timestamp,
          int64_t post_id) { (p.second)[timestamp] = post_id; },
      timestamp, post_id);
}

std::vector<Post> UserTimelineService::ReadUserTimeline(int64_t user_id,
                                                        int start, int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  auto post_ids = _userid_to_timeline_map.apply(
      user_id,
      +[](std::pair<const int64_t, Tree> &p, int start, int stop) {
        auto start_iter = p.second.find_by_order(start);
        auto stop_iter = p.second.find_by_order(stop);
        std::vector<int64_t> post_ids;
        for (auto iter = start_iter; iter != stop_iter; iter++) {
          post_ids.push_back(iter->second);
        }
        return post_ids;
      },
      start, stop);
  return _post_storage_service_obj.run(&PostStorageService::ReadPosts,
                                       std::move(post_ids));
}

}  // namespace social_network

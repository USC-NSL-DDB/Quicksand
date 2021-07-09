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
  // TODO: need synchronization.
  // TODO: computation offloading.
  auto timeline_tree_optional = _userid_to_timeline_map.get(user_id);
  auto timeline_tree =
      timeline_tree_optional ? std::move(*timeline_tree_optional) : Tree();
  timeline_tree[timestamp] = post_id;
  _userid_to_timeline_map.put(user_id, std::move(timeline_tree));
}

std::vector<Post> UserTimelineService::ReadUserTimeline(int64_t user_id,
                                                        int start, int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  // TODO: need synchronization.
  // TODO: computation offloading.
  std::vector<int64_t> post_ids;
  auto timeline_tree_optional = _userid_to_timeline_map.get(user_id);
  auto timeline_tree =
      timeline_tree_optional ? std::move(*timeline_tree_optional) : Tree();
  auto start_iter = timeline_tree.find_by_order(start);
  auto stop_iter = timeline_tree.find_by_order(stop);
  for (auto iter = start_iter; iter != stop_iter; iter++) {
    post_ids.push_back(iter->second);
  }
  return _post_storage_service_obj.run(&PostStorageService::ReadPosts,
                                       std::move(post_ids));
}

}  // namespace social_network

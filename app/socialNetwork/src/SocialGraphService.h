#pragma once

#include <algorithm>
#include <cereal/types/set.hpp>
#include <chrono>
#include <future>
#include <iostream>
#include <iterator>
#include <nu/dis_hash_table.hpp>
#include <nu/rem_obj.hpp>
#include <set>
#include <string>
#include <thread>

#include "UserService.h"

namespace social_network {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class SocialGraphService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;

  SocialGraphService(nu::RemObj<UserService>::Cap);
  std::vector<int64_t> GetFollowers(int64_t);
  std::vector<int64_t> GetFollowees(int64_t);
  void Follow(int64_t, int64_t);
  void Unfollow(int64_t, int64_t);
  void FollowWithUsername(std::string &&, std::string &&);
  void UnfollowWithUsername(std::string &&, std::string &&);

private:
  nu::RemObj<UserService> _user_service_obj;
  nu::DistributedHashTable<int64_t, std::set<int64_t>> _userid_to_followers_map;
  nu::DistributedHashTable<int64_t, std::set<int64_t>> _userid_to_followees_map;
};

SocialGraphService::SocialGraphService(nu::RemObj<UserService>::Cap cap)
    : _user_service_obj(cap),
      _userid_to_followers_map(kDefaultHashTablePowerNumShards),
      _userid_to_followees_map(kDefaultHashTablePowerNumShards) {}

void SocialGraphService::Follow(int64_t user_id, int64_t followee_id) {
  // TODO: offloading.
  auto followees_set_optional = _userid_to_followees_map.get(user_id);
  auto followees_set = followees_set_optional
                           ? std::move(*followees_set_optional)
                           : std::set<int64_t>();
  followees_set.emplace(followee_id);
  _userid_to_followees_map.put(user_id, std::move(followees_set));

  auto followers_set_optional = _userid_to_followers_map.get(followee_id);
  auto followers_set = followers_set_optional
                           ? std::move(*followers_set_optional)
                           : std::set<int64_t>();
  followers_set.emplace(user_id);
  _userid_to_followers_map.put(followee_id, std::move(followers_set));
}

void SocialGraphService::Unfollow(int64_t user_id, int64_t followee_id) {
  // TODO: offloading.
  auto followees_set_optional = _userid_to_followees_map.get(user_id);
  auto followees_set = followees_set_optional
                           ? std::move(*followees_set_optional)
                           : std::set<int64_t>();
  BUG_ON(!followees_set.erase(followee_id));
  _userid_to_followees_map.put(user_id, std::move(followees_set));

  auto followers_set_optional = _userid_to_followers_map.get(followee_id);
  auto followers_set = followers_set_optional
                           ? std::move(*followers_set_optional)
                           : std::set<int64_t>();
  BUG_ON(!followers_set.erase(user_id));
  _userid_to_followers_map.put(followee_id, std::move(followers_set));
}

std::vector<int64_t> SocialGraphService::GetFollowers(int64_t user_id) {
  std::vector<int64_t> followers_vec;
  auto followers_set_optional = _userid_to_followers_map.get(user_id);
  auto followers_set = followers_set_optional
                           ? std::move(*followers_set_optional)
                           : std::set<int64_t>();
  std::copy(followers_set.begin(), followers_set.end(),
            std::back_inserter(followers_vec));
  return followers_vec;
}

std::vector<int64_t> SocialGraphService::GetFollowees(int64_t user_id) {
  std::vector<int64_t> followees_vec;
  auto followees_set_optional = _userid_to_followees_map.get(user_id);
  auto followees_set = followees_set_optional
                           ? std::move(*followees_set_optional)
                           : std::set<int64_t>();
  std::copy(followees_set.begin(), followees_set.end(),
            std::back_inserter(followees_vec));
  return followees_vec;
}

void SocialGraphService::FollowWithUsername(std::string &&user_name,
                                            std::string &&followee_name) {
  auto user_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, std::move(user_name));
  auto followee_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, std::move(followee_name));
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Follow(user_id, followee_id);
  }
}

void SocialGraphService::UnfollowWithUsername(std::string &&user_name,
                                              std::string &&followee_name) {
  auto user_id_future = _user_service_obj.run_async(&UserService::GetUserId,
                                                    std::move(user_name));
  auto followee_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, std::move(followee_name));
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Unfollow(user_id, followee_id);
  }
}

} // namespace social_network

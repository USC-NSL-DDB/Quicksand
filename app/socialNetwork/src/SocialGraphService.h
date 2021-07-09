#pragma once

#include <algorithm>
#include <chrono>
#include <future>
#include <iostream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <nu/rem_obj.hpp>

#include "UserService.h"

namespace social_network {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class SocialGraphService {
public:
  SocialGraphService(nu::RemObj<UserService>::Cap);
  std::vector<int64_t> GetFollowers(int64_t);
  std::vector<int64_t> GetFollowees(int64_t);
  void Follow(int64_t, int64_t);
  void Unfollow(int64_t, int64_t);
  void FollowWithUsername(std::string &&, std::string &&);
  void UnfollowWithUsername(std::string &&, std::string &&);

private:
  nu::RemObj<UserService> _user_service_obj;
  // TODO: replace it with DistributedHashTable.
  std::map<int64_t, std::set<int64_t>> _userid_to_followers_map;
  std::map<int64_t, std::set<int64_t>> _userid_to_followees_map;
};

SocialGraphService::SocialGraphService(nu::RemObj<UserService>::Cap cap)
    : _user_service_obj(cap) {}

void SocialGraphService::Follow(int64_t user_id, int64_t followee_id) {
  _userid_to_followees_map[user_id].emplace(followee_id);
  _userid_to_followers_map[followee_id].emplace(user_id);
}

void SocialGraphService::Unfollow(int64_t user_id, int64_t followee_id) {
  _userid_to_followees_map[user_id].erase(followee_id);
  _userid_to_followers_map[followee_id].erase(user_id);
}

std::vector<int64_t> SocialGraphService::GetFollowers(int64_t user_id) {
  std::vector<int64_t> followers_vec;
  auto &followers_set = _userid_to_followers_map[user_id];
  std::copy(followers_set.begin(), followers_set.end(),
            std::back_inserter(followers_vec));
  return followers_vec;
}

std::vector<int64_t> SocialGraphService::GetFollowees(int64_t user_id) {
  std::vector<int64_t> followees_vec;
  auto &followees_set = _userid_to_followees_map[user_id];
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

}  // namespace social_network

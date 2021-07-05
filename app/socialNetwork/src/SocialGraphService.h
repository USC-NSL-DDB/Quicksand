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

#include "../gen-cpp/SocialGraphService.h"
#include "../gen-cpp/UserService.h"
#include "UserService.h"

#include <nu/rem_obj.hpp>

namespace social_network {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class SocialGraphService {
public:
  SocialGraphService(nu::RemObj<UserService>::Cap);
  std::vector<int64_t> GetFollowers(int64_t, int64_t);
  std::vector<int64_t> GetFollowees(int64_t, int64_t);
  void Follow(int64_t, int64_t, int64_t);
  void Unfollow(int64_t, int64_t, int64_t);
  void FollowWithUsername(int64_t, std::string &&, std::string &&);
  void UnfollowWithUsername(int64_t, std::string &&, std::string &&);

private:
  nu::RemObj<UserService> _user_service_obj;
  // TODO: replace it with DistributedHashTable.
  std::map<int64_t, std::set<int64_t>> _userid_to_followers_map;
  std::map<int64_t, std::set<int64_t>> _userid_to_followees_map;
};

SocialGraphService::SocialGraphService(nu::RemObj<UserService>::Cap cap)
    : _user_service_obj(cap) {}

void SocialGraphService::Follow(int64_t req_id, int64_t user_id,
                                int64_t followee_id) {
  std::cout << "Follow " << user_id << " " << followee_id << std::endl;
  _userid_to_followees_map[user_id].emplace(followee_id);
  _userid_to_followers_map[followee_id].emplace(user_id);
}

void SocialGraphService::Unfollow(int64_t req_id, int64_t user_id,
                                  int64_t followee_id) {
  _userid_to_followees_map[user_id].erase(followee_id);
  _userid_to_followers_map[followee_id].erase(user_id);
}

std::vector<int64_t> SocialGraphService::GetFollowers(int64_t req_id,
                                                      int64_t user_id) {
  std::cout << "GetFollowers " << user_id << std::endl;
  std::vector<int64_t> followers_vec;
  auto &followers_set = _userid_to_followers_map[user_id];
  std::copy(followers_set.begin(), followers_set.end(),
            std::back_inserter(followers_vec));
  for (auto id : followers_vec) {
    std::cout << id << ", ";
  }
  std::cout << std::endl;
  return followers_vec;
}

std::vector<int64_t> SocialGraphService::GetFollowees(int64_t req_id,
                                                      int64_t user_id) {
  std::cout << "GetFollowees " << user_id << std::endl;
  std::vector<int64_t> followees_vec;
  auto &followees_set = _userid_to_followees_map[user_id];
  std::copy(followees_set.begin(), followees_set.end(),
            std::back_inserter(followees_vec));
  for (auto id : followees_vec) {
    std::cout << id << ", ";
  }
  std::cout << std::endl;
  return followees_vec;
}

void SocialGraphService::FollowWithUsername(int64_t req_id,
                                            std::string &&user_name,
                                            std::string &&followee_name) {
  std::cout << "FollowWithUsername " << user_name << " " << followee_name
            << std::endl;
  auto user_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, req_id, std::move(user_name));
  auto followee_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, req_id, std::move(followee_name));
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Follow(req_id, user_id, followee_id);
  }
}

void SocialGraphService::UnfollowWithUsername(int64_t req_id,
                                              std::string &&user_name,
                                              std::string &&followee_name) {
  auto user_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, req_id, std::move(user_name));
  auto followee_id_future = _user_service_obj.run_async(
      &UserService::GetUserId, req_id, std::move(followee_name));
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Unfollow(req_id, user_id, followee_id);
  }
}

}  // namespace social_network

#pragma once

#include <cereal/archives/binary.hpp>
#include <cereal/types/set.hpp>
#include <cereal/types/string.hpp>
#include <ext/pb_ds/assoc_container.hpp>
#include <nu/dis_hash_table.hpp>
#include <nu/mutex.hpp>
#include <random>
#include <string>
#include <variant>
#include <vector>

#include "../gen-cpp/BackEndService.h"
#include "../gen-cpp/social_network_types.h"
#include "defs.hpp"

namespace social_network {

class BackEndService {
public:
  BackEndService(const std::string &machine_id, const std::string &secret);
  void ComposePost(const std::string &username, int64_t user_id,
                   const std::string &text,
                   const std::vector<int64_t> &media_ids,
                   const std::vector<std::string> &media_types,
                   PostType::type post_type);
  std::vector<Post> ReadUserTimeline(int64_t user_id, int start, int stop);
  std::variant<LoginErrorCode, std::string> Login(const std::string &username,
                                                  const std::string &password);
  void RegisterUser(const std::string &_first_name,
                    const std::string &_last_name, const std::string &_username,
                    const std::string &_password);
  void RegisterUserWithId(const std::string &_first_name,
                          const std::string &_last_name,
                          const std::string &_username,
                          const std::string &_password, const int64_t user_id);
  std::vector<int64_t> GetFollowers(int64_t user_id);
  void Unfollow(int64_t user_id, int64_t followee_id);
  void UnfollowWithUsername(const std::string &user_name,
                            const std::string &followee_name);
  void Follow(int64_t user_id, int64_t followee_id);
  void FollowWithUsername(const std::string &user_name,
                          const std::string &followee_name);
  std::vector<int64_t> GetFollowees(int64_t user_id);
  std::vector<Post> ReadHomeTimeline(int64_t user_id, int start, int stop);
  void UploadMedia(const std::string &filename, const std::string &data);
  std::string GetMedia(const std::string &filename);

private:
  TextServiceReturn ComposeText(const std::string &text);
  std::vector<UserMention>
  ComposeUserMentions(const std::vector<std::string> &usernames);
  std::vector<Url> ComposeUrls(const std::vector<std::string> &urls);
  void WriteUserTimeline(int64_t post_id, int64_t user_id, int64_t timestamp);
  void WriteHomeTimeline(int64_t post_id, int64_t user_id, int64_t timestamp,
                         const std::vector<int64_t> &user_mentions_id);
  std::vector<Post> ReadPosts(const std::vector<int64_t> &post_ids);
  int GetCounter(int64_t timestamp);
  int64_t ComposeUniqueId(const PostType::type post_type);
  Creator ComposeCreatorWithUserId(int64_t user_id,
                                   const std::string &username);
  Creator ComposeCreatorWithUsername(const std::string &username);
  int64_t GetUserId(const std::string &username);

  using Timeline =
      __gnu_pbds::tree<int64_t, int64_t, std::greater<int64_t>,
                       __gnu_pbds::rb_tree_tag,
                       __gnu_pbds::tree_order_statistics_node_update>;

  constexpr static uint32_t kHashTablePowerNumShards = 9;

  nu::DistributedHashTable<std::string, UserProfile>
      username_to_userprofile_map_;
  nu::DistributedHashTable<std::string, std::string> filename_to_data_map_;
  nu::DistributedHashTable<std::string, std::string> short_to_extended_map_;
  nu::DistributedHashTable<int64_t, Timeline> userid_to_hometimeline_map_;
  nu::DistributedHashTable<int64_t, Timeline> userid_to_usertimeline_map_;
  nu::DistributedHashTable<int64_t, Post> postid_to_post_map_;
  nu::DistributedHashTable<int64_t, std::set<int64_t>> userid_to_followers_map_;
  nu::DistributedHashTable<int64_t, std::set<int64_t>> userid_to_followees_map_;

  std::mt19937 generator_;
  std::uniform_int_distribution<int> distribution_;
  nu::Mutex mutex_;
  int64_t current_timestamp_ = -1;
  int counter_ = 0;
  std::string machine_id_;
  std::string secret_;
};
} // namespace social_network

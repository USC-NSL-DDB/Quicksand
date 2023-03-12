#include "states.hpp"

namespace social_network {

States::States() {}

States make_states() {
  States states;

  states.username_to_userprofile_map =
      nu::make_dis_hash_table<std::string, UserProfile, StrHasher>(
          States::kHashTablePowerNumShards);
  states.filename_to_data_map =
      nu::make_dis_hash_table<std::string, std::string, StrHasher>(
          States::kHashTablePowerNumShards);
  states.short_to_extended_map =
      nu::make_dis_hash_table<std::string, std::string, StrHasher>(
          States::kHashTablePowerNumShards);
  states.userid_to_hometimeline_map =
      nu::make_dis_hash_table<int64_t, Timeline, I64Hasher>(
          States::kHashTablePowerNumShards);
  states.userid_to_usertimeline_map =
      nu::make_dis_hash_table<int64_t, Timeline, I64Hasher>(
          States::kHashTablePowerNumShards);
  states.postid_to_post_map = nu::make_dis_hash_table<int64_t, Post, I64Hasher>(
      States::kHashTablePowerNumShards);
  states.userid_to_followers_map =
      nu::make_dis_hash_table<int64_t, std::set<int64_t>, I64Hasher>(
          States::kHashTablePowerNumShards);
  states.userid_to_followees_map =
      nu::make_dis_hash_table<int64_t, std::set<int64_t>, I64Hasher>(
          States::kHashTablePowerNumShards);

  return states;
}

}  // namespace social_network

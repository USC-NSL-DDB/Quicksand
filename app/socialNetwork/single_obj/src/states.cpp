#include "states.hpp"

namespace social_network {

States::States()
    : username_to_userprofile_map(
          nu::make_dis_hash_table<std::string, UserProfile,
                                  decltype(kHashStrtoU64)>(
              kHashTablePowerNumShards)),
      filename_to_data_map(nu::make_dis_hash_table<std::string, std::string,
                                                   decltype(kHashStrtoU64)>(
          kHashTablePowerNumShards)),
      short_to_extended_map(nu::make_dis_hash_table<std::string, std::string,
                                                    decltype(kHashStrtoU64)>(
          kHashTablePowerNumShards)),
      userid_to_hometimeline_map(
          nu::make_dis_hash_table<int64_t, Timeline, decltype(kHashI64toU64)>(
              kHashTablePowerNumShards)),
      userid_to_usertimeline_map(
          nu::make_dis_hash_table<int64_t, Timeline, decltype(kHashI64toU64)>(
              kHashTablePowerNumShards)),
      postid_to_post_map(
          nu::make_dis_hash_table<int64_t, Post, decltype(kHashI64toU64)>(
              kHashTablePowerNumShards)),
      userid_to_followers_map(
          nu::make_dis_hash_table<int64_t, std::set<int64_t>,
                                  decltype(kHashI64toU64)>(
              kHashTablePowerNumShards)),
      userid_to_followees_map(
          nu::make_dis_hash_table<int64_t, std::set<int64_t>,
                                  decltype(kHashI64toU64)>(
              kHashTablePowerNumShards)) {}

}  // namespace social_network

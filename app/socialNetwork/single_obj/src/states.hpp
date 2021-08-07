#pragma once

#include <cereal/types/string.hpp>
#include <nu/dis_hash_table.hpp>
#include <nu/rem_obj.hpp>
#include <nu/utils/farmhash.hpp>

#include "defs.hpp"
#include "utils.hpp"

namespace social_network {

struct StateCaps;

struct States {
  States();
  States(const StateCaps &caps);
  StateCaps get_caps();

  constexpr static uint32_t kHashTablePowerNumShards = 9;
  constexpr static auto kHashStrtoU64 = [](const std::string &str) {
    return util::Hash64(str);
  };
  constexpr static auto kHashI64toU64 = [](int64_t id) {
    return util::Hash64(reinterpret_cast<const char *>(&id), sizeof(int64_t));
  };

  nu::DistributedHashTable<std::string, UserProfile, decltype(kHashStrtoU64)>
      username_to_userprofile_map;
  nu::DistributedHashTable<std::string, std::string, decltype(kHashStrtoU64)>
      filename_to_data_map;
  nu::DistributedHashTable<std::string, std::string, decltype(kHashStrtoU64)>
      short_to_extended_map;
  nu::DistributedHashTable<int64_t, Timeline, decltype(kHashI64toU64)>
      userid_to_hometimeline_map;
  nu::DistributedHashTable<int64_t, Timeline, decltype(kHashI64toU64)>
      userid_to_usertimeline_map;
  nu::DistributedHashTable<int64_t, Post, decltype(kHashI64toU64)>
      postid_to_post_map;
  nu::DistributedHashTable<int64_t, std::set<int64_t>, decltype(kHashI64toU64)>
      userid_to_followers_map;
  nu::DistributedHashTable<int64_t, std::set<int64_t>, decltype(kHashI64toU64)>
      userid_to_followees_map;
  std::string secret;
};

struct StateCaps {
  template <class Archive> void serialize(Archive &ar) {
    ar(username_to_userprofile_map_cap, filename_to_data_map_cap,
       short_to_extended_map_cap, userid_to_hometimeline_map_cap,
       userid_to_usertimeline_map_cap, postid_to_post_map_cap,
       userid_to_followers_map_cap, userid_to_followees_map_cap, secret);
  }

  nu::DistributedHashTable<std::string, UserProfile,
                           decltype(States::kHashStrtoU64)>::Cap
      username_to_userprofile_map_cap;
  nu::DistributedHashTable<std::string, std::string,
                           decltype(States::kHashStrtoU64)>::Cap
      filename_to_data_map_cap;
  nu::DistributedHashTable<std::string, std::string,
                           decltype(States::kHashStrtoU64)>::Cap
      short_to_extended_map_cap;
  nu::DistributedHashTable<int64_t, Timeline,
                           decltype(States::kHashI64toU64)>::Cap
      userid_to_hometimeline_map_cap;
  nu::DistributedHashTable<int64_t, Timeline,
                           decltype(States::kHashI64toU64)>::Cap
      userid_to_usertimeline_map_cap;
  nu::DistributedHashTable<int64_t, Post, decltype(States::kHashI64toU64)>::Cap
      postid_to_post_map_cap;
  nu::DistributedHashTable<int64_t, std::set<int64_t>,
                           decltype(States::kHashI64toU64)>::Cap
      userid_to_followers_map_cap;
  nu::DistributedHashTable<int64_t, std::set<int64_t>,
                           decltype(States::kHashI64toU64)>::Cap
      userid_to_followees_map_cap;
  std::string secret;
};
} // namespace social_network

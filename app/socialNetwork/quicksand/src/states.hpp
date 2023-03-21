#pragma once

#include <cereal/types/string.hpp>
#include <cereal/types/pbds_tree.hpp>
#include <nu/sharded_unordered_map.hpp>
#include <nu/utils/farmhash.hpp>

#include "defs.hpp"
#include "utils.hpp"

namespace social_network {

struct StrHasher {
  uint64_t operator()(const std::string &str) const {
    return util::Hash64(str);
  }
};

struct I64Hasher {
  uint64_t operator()(const int64_t &id) const {
    return util::Hash64(reinterpret_cast<const char *>(&id), sizeof(int64_t));
  }
};

struct States {
  States();
  States(const States &) noexcept = default;
  States(States &&) noexcept = default;
  States &operator=(const States &) noexcept = default;
  States &operator=(States &&) noexcept = default;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(username_to_userprofile_map, filename_to_data_map, short_to_extended_map,
       userid_to_hometimeline_map, userid_to_usertimeline_map,
       postid_to_post_map, userid_to_followers_map, userid_to_followees_map,
       secret);
  }

  nu::ShardedUnorderedMap<std::string, UserProfile, StrHasher, std::true_type>
      username_to_userprofile_map;
  nu::ShardedUnorderedMap<std::string, std::string, StrHasher, std::true_type>
      filename_to_data_map;
  nu::ShardedUnorderedMap<std::string, std::string, StrHasher, std::true_type>
      short_to_extended_map;
  nu::ShardedUnorderedMap<int64_t, Timeline, I64Hasher, std::true_type>
      userid_to_hometimeline_map;
  nu::ShardedUnorderedMap<int64_t, Timeline, I64Hasher, std::true_type>
      userid_to_usertimeline_map;
  nu::ShardedUnorderedMap<int64_t, Post, I64Hasher, std::true_type>
      postid_to_post_map;
  nu::ShardedUnorderedMap<int64_t, std::set<int64_t>, I64Hasher, std::true_type>
      userid_to_followers_map;
  nu::ShardedUnorderedMap<int64_t, std::set<int64_t>, I64Hasher, std::true_type>
      userid_to_followees_map;
  std::string secret;
};

States make_states();

} // namespace social_network

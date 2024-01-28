#include "states.hpp"

namespace social_network {

States::States() {}

States make_states() {
  States states;

  states.username_to_userprofile_map =
      nu::make_sharded_ts_umap<std::string, UserProfile, StrHasher>(257 / 20);
  states.filename_to_data_map =
      nu::make_sharded_ts_umap<std::string, std::string, StrHasher>(1);
  states.short_to_extended_map =
      nu::make_sharded_ts_umap<std::string, std::string, StrHasher>(1);
  states.userid_to_hometimeline_map =
      nu::make_sharded_ts_umap<int64_t, Timeline, I64Hasher>(2623 / 20);
  states.userid_to_usertimeline_map =
      nu::make_sharded_ts_umap<int64_t, Timeline, I64Hasher>(1025 / 20);
  states.postid_to_post_map =
      nu::make_sharded_ts_umap<int64_t, Post, I64Hasher>(18124 / 20);
  states.userid_to_followers_map =
      nu::make_sharded_ts_umap<int64_t, std::set<int64_t>, I64Hasher>(282 / 20);
  states.userid_to_followees_map =
      nu::make_sharded_ts_umap<int64_t, std::set<int64_t>, I64Hasher>(1);

  return states;
}

}  // namespace social_network

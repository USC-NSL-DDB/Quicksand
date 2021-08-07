#include "states.hpp"

namespace social_network {

States::States()
    : username_to_userprofile_map(kHashTablePowerNumShards),
      filename_to_data_map(kHashTablePowerNumShards),
      short_to_extended_map(kHashTablePowerNumShards),
      userid_to_hometimeline_map(kHashTablePowerNumShards),
      userid_to_usertimeline_map(kHashTablePowerNumShards),
      postid_to_post_map(kHashTablePowerNumShards),
      userid_to_followers_map(kHashTablePowerNumShards),
      userid_to_followees_map(kHashTablePowerNumShards) {}

States::States(const StateCaps &caps)
    : username_to_userprofile_map(caps.username_to_userprofile_map_cap),
      filename_to_data_map(caps.filename_to_data_map_cap),
      short_to_extended_map(caps.short_to_extended_map_cap),
      userid_to_hometimeline_map(caps.userid_to_hometimeline_map_cap),
      userid_to_usertimeline_map(caps.userid_to_usertimeline_map_cap),
      postid_to_post_map(caps.postid_to_post_map_cap),
      userid_to_followers_map(caps.userid_to_followers_map_cap),
      userid_to_followees_map(caps.userid_to_followees_map_cap),
      secret(caps.secret) {}

StateCaps States::get_caps() {
  StateCaps caps;
  caps.username_to_userprofile_map_cap = username_to_userprofile_map.get_cap();
  caps.filename_to_data_map_cap = filename_to_data_map.get_cap();
  caps.short_to_extended_map_cap = short_to_extended_map.get_cap();
  caps.userid_to_hometimeline_map_cap = userid_to_hometimeline_map.get_cap();
  caps.userid_to_usertimeline_map_cap = userid_to_usertimeline_map.get_cap();
  caps.postid_to_post_map_cap = postid_to_post_map.get_cap();
  caps.userid_to_followers_map_cap = userid_to_followers_map.get_cap();
  caps.userid_to_followees_map_cap = userid_to_followees_map.get_cap();
  caps.secret = secret;
  return caps;
}
} // namespace social_network

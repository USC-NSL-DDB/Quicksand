#pragma once

#include <cstdint>
#include <string>

#include "../gen-cpp/social_network_types.h"
#include "UserService.h"

namespace social_network {

class UserMentionService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;

  UserMentionService(UserService::UserProfileMap::Cap &&cap);
  std::vector<UserMention> ComposeUserMentions(std::vector<std::string> &&);

private:
  UserService::UserProfileMap _username_to_userid_map;
};

UserMentionService::UserMentionService(UserService::UserProfileMap::Cap &&cap)
    : _username_to_userid_map(std::move(cap)) {}

std::vector<UserMention>
UserMentionService::ComposeUserMentions(std::vector<std::string> &&usernames) {
  std::vector<UserMention> user_mentions;

  for (auto &username : usernames) {
    UserMention user_mention;
    user_mention.username = username;
    auto user_id_optional = _username_to_userid_map.get(username);
    BUG_ON(!user_id_optional);
    user_mention.user_id = user_id_optional->user_id;
    user_mentions.push_back(user_mention);
  }
  return user_mentions;
}

}  // namespace social_network

#pragma once

#include <cstdint>
#include <string>

#include "../gen-cpp/social_network_types.h"

namespace social_network {

class UserMentionService {
public:
  std::vector<UserMention> ComposeUserMentions(std::vector<std::string> &&);

private:
  // TODO: use DistributedHashTable.
  // TODO: should connect with UserService's hashtable.
  std::map<std::string, int64_t> _username_to_userid_map;
};

std::vector<UserMention>
UserMentionService::ComposeUserMentions(std::vector<std::string> &&usernames) {
  std::vector<UserMention> user_mentions;

  for (auto &username : usernames) {
    UserMention user_mention;
    user_mention.username = username;
    user_mention.user_id = _username_to_userid_map[username];
    user_mentions.push_back(user_mention);
  }
  return user_mentions;
}

}  // namespace social_network

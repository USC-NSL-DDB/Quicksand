#pragma once

#include <chrono>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <nu/mutex.hpp>
#include <string>
#include <vector>

#include "../gen-cpp/social_network_types.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using json = nlohmann::json;

namespace social_network {

int LoadConfigFile(const std::string &file_name, json *config_json);
u_int16_t HashMacAddressPid(const std::string &mac);
std::string GenRandomString(const int len);
std::string GetMachineId(std::string &netif);
int64_t GenUserId(const std::string &machine_id, int64_t timestamp,
                  int64_t counter);
bool VerifyLogin(std::string &signature, const UserProfile &user_profile,
                 const std::string &username, const std::string &password,
                 const std::string &secret);
std::vector<std::string> MatchUrls(const std::string &text);
std::vector<std::string> MatchMentions(const std::string &text);
std::string ShortenUrlInText(const std::string &text,
                             std::vector<Url> target_urls);
int64_t GenUniqueId();

} // namespace social_network

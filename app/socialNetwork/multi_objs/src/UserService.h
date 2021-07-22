#pragma once

#include <cereal/types/variant.hpp>
#include <iomanip>
#include <iostream>
#include <jwt/jwt.hpp>
#include <nlohmann/json.hpp>
#include <nu/mutex.hpp>
#include <nu/rem_obj.hpp>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>
extern "C" {
#include <runtime/preempt.h>
#include <runtime/timer.h>
}

#include "../gen-cpp/social_network_types.h"
#include "../third_party/PicoSHA2/picosha2.h"
#include "utils.h"

// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000
#define MONGODB_TIMEOUT_MS 100

namespace social_network {

using json = nlohmann::json;
using namespace jwt::params;

std::string GenRandomString(const int len) {
  static const std::string alphanum = "0123456789"
                                      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                      "abcdefghijklmnopqrstuvwxyz";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(
      0, static_cast<int>(alphanum.length() - 1));
  std::string s;
  for (int i = 0; i < len; ++i) {
    s += alphanum[dist(gen)];
  }
  return s;
}

struct UserProfile {
  int64_t user_id;
  std::string first_name;
  std::string last_name;
  std::string salt;
  std::string password_hashed;

  template <class Archive> void serialize(Archive &ar) {
    ar(user_id, first_name, last_name, salt, password_hashed);
  }
};

enum LoginErrorCode { OK, NOT_REGISTERED, WRONG_PASSWORD };

class UserService {
public:
  constexpr static uint32_t kDefaultHashTablePowerNumShards = 9;
  using UserProfileMap = nu::DistributedHashTable<std::string, UserProfile>;

  UserService(UserProfileMap::Cap &&cap);
  void RegisterUser(std::string, std::string, std::string, std::string);
  void RegisterUserWithId(std::string, std::string, std::string, std::string,
                          int64_t);
  Creator ComposeCreatorWithUserId(int64_t, std::string);
  Creator ComposeCreatorWithUsername(std::string);
  std::variant<LoginErrorCode, std::string> Login(std::string, std::string);
  int64_t GetUserId(std::string);

private:
  std::string _machine_id;
  std::string _secret;
  UserProfileMap _username_to_userprofile_map;
  nu::Mutex _mutex;
};

UserService::UserService(UserProfileMap::Cap &&cap)
    : _username_to_userprofile_map(std::move(cap)) {
  json config_json;
  BUG_ON(load_config_file("config/service-config.json", &config_json) != 0);
  _secret = config_json["secret"];
  std::string netif = config_json["user-service"]["netif"];
  _machine_id = GetMachineId(netif);
  BUG_ON(_machine_id == "");
}

void UserService::RegisterUserWithId(std::string first_name,
                                     std::string last_name,
                                     std::string username, std::string password,
                                     int64_t user_id) {
  UserProfile user_profile;
  user_profile.first_name = std::move(first_name);
  user_profile.last_name = std::move(last_name);
  user_profile.user_id = user_id;
  user_profile.salt = GenRandomString(32);
  user_profile.password_hashed =
      picosha2::hash256_hex_string(std::move(password) + user_profile.salt);
  _username_to_userprofile_map.put(std::move(username),
                                   std::move(user_profile));
}

void UserService::RegisterUser(std::string first_name, std::string last_name,
                               std::string username, std::string password) {
  // Compose user_id
  auto core_id = read_cpu();
  auto tsc = rdtsc();
  auto user_id = (((tsc << 8) | core_id) << 16);
  RegisterUserWithId(std::move(first_name), std::move(last_name),
                     std::move(username), std::move(password), user_id);
}

Creator UserService::ComposeCreatorWithUsername(std::string username) {
  auto user_id_optional = _username_to_userprofile_map.get(username);
  BUG_ON(!user_id_optional);
  return ComposeCreatorWithUserId(user_id_optional->user_id,
                                  std::move(username));
}

Creator UserService::ComposeCreatorWithUserId(int64_t user_id,
                                              std::string username) {
  Creator creator;
  creator.username = username;
  creator.user_id = user_id;
  return creator;
}

std::variant<LoginErrorCode, std::string>
UserService::Login(std::string username, std::string password) {
  auto user_profile_optional = _username_to_userprofile_map.get(username);
  if (!user_profile_optional) {
    return NOT_REGISTERED;
  }
  auto &user_profile = *user_profile_optional;
  bool auth =
      (picosha2::hash256_hex_string(std::move(password) + user_profile.salt) ==
       user_profile.password_hashed);
  if (!auth) {
    return WRONG_PASSWORD;
  }
  auto user_id_str = std::to_string(user_profile.user_id);
  auto timestamp_str =
      std::to_string(duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count());
  jwt::jwt_object obj{algorithm("HS256"), secret(_secret),
                      payload({{"user_id", user_id_str},
                               {"username", username},
                               {"timestamp", timestamp_str},
                               {"ttl", "3600"}})};
  return obj.signature();
}

int64_t UserService::GetUserId(std::string username) {
  auto user_id_optional = _username_to_userprofile_map.get(std::move(username));
  BUG_ON(!user_id_optional);
  return user_id_optional->user_id;
}

} // namespace social_network

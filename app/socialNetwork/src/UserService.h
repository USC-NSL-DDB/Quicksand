#pragma once

#include <cereal/types/variant.hpp>
#include <iomanip>
#include <iostream>
#include <jwt/jwt.hpp>
#include <map>
#include <nlohmann/json.hpp>
#include <nu/mutex.hpp>
#include <nu/rem_obj.hpp>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>

#include "../gen-cpp/social_network_types.h"
#include "../third_party/PicoSHA2/picosha2.h"
#include "utils.h"

// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000
#define MONGODB_TIMEOUT_MS 100

namespace social_network {

using json = nlohmann::json;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using namespace jwt::params;

static int64_t current_timestamp = -1;
static int counter = 0;

static int GetCounter(int64_t timestamp) {
  if (current_timestamp > timestamp) {
    std::cerr << "Timestamps are not incremental." << std::endl;
    exit(EXIT_FAILURE);
  }
  if (current_timestamp == timestamp) {
    return counter++;
  } else {
    current_timestamp = timestamp;
    counter = 0;
    return counter++;
  }
}

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

/*
 * The following code which obtaines machine ID from machine's MAC address was
 * inspired from https://stackoverflow.com/a/16859693.
 *
 * MAC address is obtained from /sys/class/net/<netif>/address
 */
u_int16_t HashMacAddressPid(const std::string &mac) {
  u_int16_t hash = 0;
  std::string mac_pid = mac + std::to_string(getpid());
  for (unsigned int i = 0; i < mac_pid.size(); i++) {
    hash += (mac[i] << ((i & 1) * 8));
  }
  return hash;
}

std::string GetMachineId(std::string &netif) {
  std::string mac_hash;

  std::string mac_addr_filename = "/sys/class/net/" + netif + "/address";
  std::ifstream mac_addr_file;
  mac_addr_file.open(mac_addr_filename);
  if (!mac_addr_file) {
    std::cerr << "Cannot read MAC address from net interface " << netif
              << std::endl;
    return "";
  }
  std::string mac;
  mac_addr_file >> mac;
  if (mac == "") {
    std::cerr << "Cannot read MAC address from net interface " << netif
              << std::endl;
    return "";
  }
  mac_addr_file.close();

  std::cerr << "MAC address = " << mac << std::endl;

  std::stringstream stream;
  stream << std::hex << HashMacAddressPid(mac);
  mac_hash = stream.str();

  if (mac_hash.size() > 3) {
    mac_hash.erase(0, mac_hash.size() - 3);
  } else if (mac_hash.size() < 3) {
    mac_hash = std::string(3 - mac_hash.size(), '0') + mac_hash;
  }
  return mac_hash;
}

struct UserProfile {
  int64_t user_id;
  std::string first_name;
  std::string last_name;
  std::string salt;
  std::string password_hashed;
};

enum LoginErrorCode { OK, NOT_REGISTERED, WRONG_PASSWORD };

class UserService {
public:
  UserService();
  void RegisterUser(std::string &&, std::string &&, std::string &&,
                    std::string &&);
  void RegisterUserWithId(std::string &&, std::string &&, std::string &&,
                          std::string &&, int64_t);
  Creator ComposeCreatorWithUserId(int64_t, std::string &&);
  Creator ComposeCreatorWithUsername(std::string &&);
  std::variant<LoginErrorCode, std::string> Login(std::string &&,
                                                  std::string &&);
  int64_t GetUserId(std::string &&);

private:
  std::string _machine_id;
  std::string _secret;
  // TODO: replace it with DistributedHashTable.
  std::map<std::string, UserProfile> _username_to_userprofile_map;
  nu::Mutex _mutex;
};

UserService::UserService() {
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }
  _secret = config_json["secret"];
  std::string netif = config_json["user-service"]["netif"];
  _machine_id = GetMachineId(netif);
  if (_machine_id == "") {
    exit(EXIT_FAILURE);
  }
}

void UserService::RegisterUserWithId(std::string &&first_name,
                                     std::string &&last_name,
                                     std::string &&username,
                                     std::string &&password, int64_t user_id) {
  UserProfile user_profile;
  user_profile.first_name = first_name;
  user_profile.last_name = last_name;
  user_profile.user_id = user_id;
  user_profile.salt = GenRandomString(32);
  user_profile.password_hashed =
      picosha2::hash256_hex_string(password + user_profile.salt);
  _username_to_userprofile_map[username] = user_profile;
}

void UserService::RegisterUser(std::string &&first_name,
                               std::string &&last_name, std::string &&username,
                               std::string &&password) {
  // Compose user_id
  _mutex.Lock();
  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count() -
      CUSTOM_EPOCH;
  int idx = GetCounter(timestamp);
  _mutex.Unlock();

  std::stringstream sstream;
  sstream << std::hex << timestamp;
  std::string timestamp_hex(sstream.str());
  if (timestamp_hex.size() > 10) {
    timestamp_hex.erase(0, timestamp_hex.size() - 10);
  } else if (timestamp_hex.size() < 10) {
    timestamp_hex = std::string(10 - timestamp_hex.size(), '0') + timestamp_hex;
  }
  // Empty the sstream buffer.
  sstream.clear();
  sstream.str(std::string());

  sstream << std::hex << idx;
  std::string counter_hex(sstream.str());

  if (counter_hex.size() > 3) {
    counter_hex.erase(0, counter_hex.size() - 3);
  } else if (counter_hex.size() < 3) {
    counter_hex = std::string(3 - counter_hex.size(), '0') + counter_hex;
  }
  std::string user_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t user_id = stoul(user_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;

  RegisterUserWithId(std::move(first_name), std::move(last_name),
                     std::move(username), std::move(password), user_id);
}

Creator UserService::ComposeCreatorWithUsername(std::string &&username) {
  auto user_id = _username_to_userprofile_map[username].user_id;
  return ComposeCreatorWithUserId(user_id, std::move(username));
}

Creator UserService::ComposeCreatorWithUserId(int64_t user_id,
                                              std::string &&username) {
  Creator creator;
  creator.username = username;
  creator.user_id = user_id;
  return creator;
}

std::variant<LoginErrorCode, std::string>
UserService::Login(std::string &&username, std::string &&password) {
  auto user_profile_iter = _username_to_userprofile_map.find(username);
  if (user_profile_iter == _username_to_userprofile_map.end()) {
    return NOT_REGISTERED;
  }
  auto &user_profile = user_profile_iter->second;
  bool auth = (picosha2::hash256_hex_string(password + user_profile.salt) ==
               user_profile.password_hashed);
  if (!auth) {
    return WRONG_PASSWORD;
  }
  auto user_id_str = std::to_string(user_profile.user_id);
  auto timestamp_str = std::to_string(
      duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
  jwt::jwt_object obj{algorithm("HS256"), secret(_secret),
                      payload({{"user_id", user_id_str},
                               {"username", username},
                               {"timestamp", timestamp_str},
                               {"ttl", "3600"}})};
  return obj.signature();
}

int64_t UserService::GetUserId(std::string &&username) {
  return _username_to_userprofile_map[username].user_id;
}

} // namespace social_network

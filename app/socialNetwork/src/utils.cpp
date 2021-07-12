#include <PicoSHA2/picosha2.h>
#include <fstream>
#include <iostream>
#include <jwt/jwt.hpp>
#include <random>
#include <regex>
#include <sstream>
#include <string>

#include "defs.hpp"
#include "utils.hpp"

using namespace jwt::params;

namespace social_network {

int LoadConfigFile(const std::string &file_name, json *config_json) {
  std::ifstream json_file;
  json_file.open(file_name);
  if (json_file.is_open()) {
    json_file >> *config_json;
    json_file.close();
    return 0;
  } else {
    std::cerr << "Cannot open service-config.json" << std::endl;
    return -1;
  }
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

bool VerifyLogin(std::string &signature, const UserProfile &user_profile,
                 const std::string &username, const std::string &password,
                 const std::string &sec) {
  if (picosha2::hash256_hex_string(password + user_profile.salt) !=
      user_profile.password_hashed) {
    return false;
  }
  auto user_id_str = std::to_string(user_profile.user_id);
  auto timestamp_str =
      std::to_string(duration_cast<std::chrono::seconds>(
                         system_clock::now().time_since_epoch())
                         .count());
  jwt::jwt_object obj{algorithm("HS256"), secret(sec),
                      payload({{"user_id", user_id_str},
                               {"username", username},
                               {"timestamp", timestamp_str},
                               {"ttl", "3600"}})};
  signature = obj.signature();
  return true;
}

std::vector<std::string> MatchUrls(const std::string &text) {
  std::vector<std::string> urls;
  std::smatch m;
  std::regex e("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }
  return urls;
}

std::vector<std::string> MatchMentions(const std::string &text) {
  std::vector<std::string> mention_usernames;
  std::smatch m;
  std::regex e("@[a-zA-Z0-9-_]+");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }
  return mention_usernames;
}

std::string ShortenUrlInText(const std::string &text,
                             std::vector<Url> target_urls) {
  if (target_urls.empty()) {
    return text;
  }

  std::string updated_text;
  auto s = text;
  std::smatch m;
  std::regex e("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
  int idx = 0;
  while (std::regex_search(s, m, e)) {
    updated_text += m.prefix().str() + target_urls[idx].shortened_url;
    s = m.suffix().str();
    idx++;
  }
  updated_text += s;
  return updated_text;
}

UniqueIdGenerator::UniqueIdGenerator(const std::string &machine_id)
    : machine_id_(machine_id) {}

int64_t UniqueIdGenerator::Gen() {
  mutex_.Lock();
  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count() -
      CUSTOM_EPOCH;
  int idx = GetCounter(timestamp);
  mutex_.Unlock();

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
  std::string user_id_str = machine_id_ + timestamp_hex + counter_hex;
  int64_t user_id = stoul(user_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;
  return user_id;
}

int UniqueIdGenerator::GetCounter(int64_t timestamp) {
  if (current_timestamp_ > timestamp) {
    std::cerr << "Timestamps are not incremental." << std::endl;
    BUG();
  }
  if (current_timestamp_ == timestamp) {
    return counter_++;
  } else {
    current_timestamp_ = timestamp;
    counter_ = 0;
    return counter_++;
  }
}

} // namespace social_network

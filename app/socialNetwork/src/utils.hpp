#pragma once

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace social_network {

int load_config_file(const std::string &file_name, json *config_json);
u_int16_t HashMacAddressPid(const std::string &mac);
std::string GenRandomString(const int len);
std::string GetMachineId(std::string &netif);

} // namespace social_network

#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

namespace social_network{
using json = nlohmann::json;

int load_config_file(const std::string &file_name, json *config_json) {
  std::ifstream json_file;
  json_file.open(file_name);
  if (json_file.is_open()) {
    json_file >> *config_json;
    json_file.close();
    return 0;
  }
  else {
    std::cerr << "Cannot open service-config.json" << std::endl;
    return -1;
  }
};

} //namespace social_network

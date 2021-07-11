/*
 * 64-bit Unique Id Generator
 *
 * ------------------------------------------------------------------------
 * |0| 11 bit machine ID |      40-bit timestamp         | 12-bit counter |
 * ------------------------------------------------------------------------
 *
 * 11-bit machine Id code by hasing the MAC address
 * 40-bit UNIX timestamp in millisecond precision with custom epoch
 * 12 bit counter which increases monotonically on single process
 *
 */

#pragma once

#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <nu/mutex.hpp>
#include <sstream>
#include <string>

#include "../gen-cpp/social_network_types.h"
#include "utils.h"

// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000

namespace social_network {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using json = nlohmann::json;

class UniqueIdService {
public:
  UniqueIdService();
  int64_t ComposeUniqueId(const PostType::type post_type);

private:
  int64_t _current_timestamp = -1;
  int _counter = 0;
  nu::Mutex _thread_lock;
  std::string _machine_id;

  int GetCounter(int64_t timestamp);
};

int UniqueIdService::GetCounter(int64_t timestamp) {
  if (_current_timestamp > timestamp) {
    std::cerr << "Timestamps are not incremental." << std::endl;
    BUG();
  }
  if (_current_timestamp == timestamp) {
    return _counter++;
  } else {
    _current_timestamp = timestamp;
    _counter = 0;
    return _counter++;
  }
}

UniqueIdService::UniqueIdService() {
  json config_json;
  BUG_ON(load_config_file("config/service-config.json", &config_json) != 0);

  std::string netif = config_json["unique-id-service"]["netif"];
  _machine_id = GetMachineId(netif);
  BUG_ON(_machine_id == "");
  std::cout << "machine_id = " << _machine_id << std::endl;
}

int64_t UniqueIdService::ComposeUniqueId(PostType::type post_type) {
  _thread_lock.Lock();
  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count() -
      CUSTOM_EPOCH;
  int idx = GetCounter(timestamp);
  _thread_lock.Unlock();

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
  std::string post_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t post_id = stoul(post_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;

  return post_id;
}

} // namespace social_network

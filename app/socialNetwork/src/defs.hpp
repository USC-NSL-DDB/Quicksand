#pragma once

#define HOSTNAME "http://short-url/"
// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000

namespace social_network {

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

} // namespace social_network

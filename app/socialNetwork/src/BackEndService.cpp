#include <cereal/types/set.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>
#include <chrono>
#include <ext/pb_ds/assoc_container.hpp>
#include <future>
#include <iostream>
#include <jwt/jwt.hpp>
#include <nlohmann/json.hpp>
#include <nu/dis_hash_table.hpp>
#include <nu/mutex.hpp>
#include <nu/rem_obj.hpp>
#include <nu/runtime.hpp>
#include <random>
#include <regex>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <variant>
#include <vector>

#include "../gen-cpp/BackEndService.h"
#include "../gen-cpp/social_network_types.h"
#include "../third_party/PicoSHA2/picosha2.h"
#include "utils.h"

#define HOSTNAME "http://short-url/"
// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace jwt::params;
using namespace social_network;

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

class BackEndHandler : public BackEndServiceIf {
public:
  BackEndHandler();
  ~BackEndHandler() override = default;

  void ComposePost(const std::string &username, int64_t user_id,
                   const std::string &text,
                   const std::vector<int64_t> &media_ids,
                   const std::vector<std::string> &media_types,
                   PostType::type post_type) override;
  void ReadUserTimeline(std::vector<Post> &, int64_t, int, int) override;
  void Login(std::string &_return, const std::string &username,
             const std::string &password) override;
  void RegisterUser(const std::string &first_name, const std::string &last_name,
                    const std::string &username,
                    const std::string &password) override;
  void RegisterUserWithId(const std::string &first_name,
                          const std::string &last_name,
                          const std::string &username,
                          const std::string &password,
                          const int64_t user_id) override;
  void GetFollowers(std::vector<int64_t> &_return,
                    const int64_t user_id) override;
  void Unfollow(const int64_t user_id, const int64_t followee_id) override;
  void UnfollowWithUsername(const std::string &user_usernmae,
                            const std::string &followee_username) override;
  void Follow(const int64_t user_id, const int64_t followee_id) override;
  void FollowWithUsername(const std::string &user_usernmae,
                          const std::string &followee_username) override;
  void GetFollowees(std::vector<int64_t> &_return,
                    const int64_t user_id) override;
  void ReadHomeTimeline(std::vector<Post> &_return, const int64_t user_id,
                        const int32_t start, const int32_t stop) override;
  void UploadMedia(const std::string &filename,
                   const std::string &data) override;
  void GetMedia(std::string &_return, const std::string &filename) override;

private:
  using Timeline =
      __gnu_pbds::tree<int64_t, int64_t, std::greater<int64_t>,
                       __gnu_pbds::rb_tree_tag,
                       __gnu_pbds::tree_order_statistics_node_update>;

  // TODO: initialized with the specified num of shards.
  nu::DistributedHashTable<std::string, UserProfile>
      _username_to_userprofile_map;
  nu::DistributedHashTable<std::string, std::string> _filename_to_data_map;
  nu::DistributedHashTable<std::string, std::string> _short_to_extended_map;
  nu::DistributedHashTable<int64_t, Timeline> _userid_to_timeline_map;
  nu::DistributedHashTable<int64_t, Post> _postid_to_post_map;
  nu::DistributedHashTable<int64_t, std::set<int64_t>> _userid_to_followers_map;
  nu::DistributedHashTable<int64_t, std::set<int64_t>> _userid_to_followees_map;

  std::mt19937 _generator;
  std::uniform_int_distribution<int> _distribution;
  nu::Mutex _mutex;
  int64_t _current_timestamp = -1;
  int _counter = 0;
  std::string _machine_id;
  std::string _secret;

  TextServiceReturn _ComposeText(const std::string &text);
  std::vector<UserMention>
  _ComposeUserMentions(const std::vector<std::string> &usernames);
  std::vector<Url> _ComposeUrls(const std::vector<std::string> &urls);
  void _WriteUserTimeline(int64_t post_id, int64_t user_id, int64_t timestamp);
  std::vector<Post> _ReadUserTimeline(int64_t user_id, int start, int stop);
  void _WriteHomeTimeline(int64_t post_id, int64_t user_id, int64_t timestamp,
                          const std::vector<int64_t> &user_mentions_id);
  std::vector<Post> _ReadHomeTimeline(int64_t user_id, int start, int stop);
  std::vector<Post> _ReadPosts(const std::vector<int64_t> &post_ids);
  std::vector<int64_t> _GetFollowers(int64_t user_id);
  std::vector<int64_t> _GetFollowees(int64_t user_id);
  void _Follow(int64_t user_id, int64_t followee_id);
  void _Unfollow(int64_t user_id, int64_t followee_id);
  void _FollowWithUsername(const std::string &user_name,
                           const std::string &followee_name);
  void _UnfollowWithUsername(const std::string &user_name,
                             const std::string &followee_name);
  int _GetCounter(int64_t timestamp);
  int64_t _ComposeUniqueId(const PostType::type post_type);
  void _RegisterUser(const std::string &_first_name,
                     const std::string &_last_name,
                     const std::string &_username,
                     const std::string &_password);
  void _RegisterUserWithId(const std::string &_first_name,
                           const std::string &_last_name,
                           const std::string &_username,
                           const std::string &_password, const int64_t user_id);
  Creator _ComposeCreatorWithUserId(int64_t user_id,
                                    const std::string &username);
  Creator _ComposeCreatorWithUsername(const std::string &username);
  std::variant<LoginErrorCode, std::string> _Login(const std::string &username,
                                                   const std::string &password);
  int64_t _GetUserId(const std::string &username);
};

BackEndHandler::BackEndHandler()
    : _generator(
          std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() %
                       0xffffffff)),
      _distribution(std::uniform_int_distribution<int>(0, 61)) {
  json config_json;
  BUG_ON(load_config_file("config/service-config.json", &config_json) != 0);
  _secret = config_json["secret"];
  std::string netif = config_json["back-end-service"]["netif"];
  _machine_id = GetMachineId(netif);
  BUG_ON(_machine_id == "");
  std::cout << "machine_id = " << _machine_id << std::endl;
}

void BackEndHandler::ComposePost(const std::string &_username, int64_t user_id,
                                 const std::string &text,
                                 const std::vector<int64_t> &_media_ids,
                                 const std::vector<std::string> &_media_types,
                                 const PostType::type post_type) {
  auto text_service_return_future =
      nu::async([&] { return _ComposeText(text); });

  auto unique_id_future =
      nu::async([&] { return _ComposeUniqueId(post_type); });

  auto media_types = _media_types;
  auto media_ids = _media_ids;
  std::vector<Media> medias;
  BUG_ON(media_types.size() != media_ids.size());
  for (int i = 0; i < media_ids.size(); ++i) {
    Media media;
    media.media_id = media_ids[i];
    media.media_type = media_types[i];
    medias.emplace_back(media);
  }  

  auto username = _username;
  auto creator = _ComposeCreatorWithUserId(user_id, username);

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  auto unique_id = unique_id_future.get();
  auto write_user_timeline_future = nu::async(
      [&] { return _WriteUserTimeline(unique_id, user_id, timestamp); });

  auto text_service_return = text_service_return_future.get();
  std::vector<int64_t> user_mention_ids;
  for (auto &item : text_service_return.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }
  auto write_home_timeline_future = nu::async([&] {
    return _WriteHomeTimeline(unique_id, user_id, timestamp, user_mention_ids);
  });

  post.text = std::move(text_service_return.text);
  post.urls = std::move(text_service_return.urls);
  post.user_mentions = std::move(text_service_return.user_mentions);
  post.post_id = unique_id;
  post.media = std::move(medias);
  post.creator = std::move(creator);
  post.post_type = post_type;

  auto post_future =
      _postid_to_post_map.put_async(post.post_id, std::move(post));

  write_user_timeline_future.get();
  post_future.get();
  write_home_timeline_future.get();
}

void BackEndHandler::ReadUserTimeline(std::vector<Post> &_return,
                                      int64_t user_id, int start, int stop) {
  _return = _ReadUserTimeline(user_id, start, stop);
}

void BackEndHandler::Login(std::string &_return, const std::string &_username,
                           const std::string &_password) {
  auto username = _username;
  auto password = _password;
  auto variant = _Login(username, password);
  if (std::holds_alternative<LoginErrorCode>(variant)) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_UNAUTHORIZED;
    auto &login_error_code = std::get<LoginErrorCode>(variant);
    switch (login_error_code) {
    case NOT_REGISTERED:
      se.message = "The username is not registered yet.";
      break;
    case WRONG_PASSWORD:
      se.message = "Wrong password.";
      break;
    default:
      break;
    }
    throw se;
  }
  _return = std::get<std::string>(variant);
}

void BackEndHandler::RegisterUser(const std::string &first_name,
                                  const std::string &last_name,
                                  const std::string &username,
                                  const std::string &password) {
  return _RegisterUser(first_name, last_name, username, password);
}

void BackEndHandler::RegisterUserWithId(const std::string &first_name,
                                        const std::string &last_name,
                                        const std::string &username,
                                        const std::string &password,
                                        const int64_t user_id) {
  return _RegisterUserWithId(first_name, last_name, username, password,
                             user_id);
}

void BackEndHandler::GetFollowers(std::vector<int64_t> &_return,
                                  const int64_t user_id) {
  _return = _GetFollowers(user_id);
}

void BackEndHandler::Unfollow(const int64_t user_id,
                              const int64_t followee_id) {
  _Unfollow(user_id, followee_id);
}

void BackEndHandler::UnfollowWithUsername(
    const std::string &user_username, const std::string &followee_username) {
  _UnfollowWithUsername(user_username, followee_username);
}

void BackEndHandler::Follow(const int64_t user_id, const int64_t followee_id) {
  _Follow(user_id, followee_id);
}

void BackEndHandler::FollowWithUsername(const std::string &user_username,
                                        const std::string &followee_username) {
  _FollowWithUsername(user_username, followee_username);
}

void BackEndHandler::GetFollowees(std::vector<int64_t> &_return,
                                  const int64_t user_id) {
  _return = _GetFollowees(user_id);
}

void BackEndHandler::ReadHomeTimeline(std::vector<Post> &_return,
                                      const int64_t user_id,
                                      const int32_t start, const int32_t stop) {
  _return = _ReadHomeTimeline(user_id, start, stop);
}

void BackEndHandler::UploadMedia(const std::string &filename,
                                 const std::string &data) {
  _filename_to_data_map.put(filename, data);
}

void BackEndHandler::GetMedia(std::string &_return,
                              const std::string &filename) {
  auto optional = _filename_to_data_map.get(filename);
  BUG_ON(!optional);
  _return = std::move(*optional);
}

TextServiceReturn BackEndHandler::_ComposeText(const std::string &text) {
  std::vector<std::string> urls;
  std::smatch m;
  std::regex e("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }
  auto target_urls_future = nu::async([&] { return _ComposeUrls(urls); });

  std::vector<std::string> mention_usernames;
  e = "@[a-zA-Z0-9-_]+";
  s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }

  auto user_mentions_future =
      nu::async([&] { return _ComposeUserMentions(mention_usernames); });

  auto target_urls = target_urls_future.get();
  std::string updated_text;
  if (!urls.empty()) {
    s = text;
    int idx = 0;
    while (std::regex_search(s, m, e)) {
      auto url = m.str();
      urls.emplace_back(url);
      updated_text += m.prefix().str() + target_urls[idx].shortened_url;
      s = m.suffix().str();
      idx++;
    }
    updated_text += s;
  } else {
    updated_text = text;
  }

  auto user_mentions = user_mentions_future.get();
  TextServiceReturn text_service_return;
  text_service_return.user_mentions = user_mentions;
  text_service_return.text = updated_text;
  text_service_return.urls = target_urls;

  return text_service_return;
}

std::vector<UserMention> BackEndHandler::_ComposeUserMentions(
    const std::vector<std::string> &usernames) {
  std::vector<nu::Future<std::optional<UserProfile>>>
      user_profile_optional_futures;
  for (auto &username : usernames) {
    user_profile_optional_futures.emplace_back(
        _username_to_userprofile_map.get_async(username));
  }

  std::vector<UserMention> user_mentions;
  for (size_t i = 0; i < user_profile_optional_futures.size(); i++) {
    auto user_profile_optional = user_profile_optional_futures[i].get();
    BUG_ON(!user_profile_optional);
    auto &user_profile = *user_profile_optional;
    user_mentions.emplace_back();
    auto &user_mention = user_mentions.back();
    user_mention.username = std::move(usernames[i]);
    user_mention.user_id = user_profile.user_id;
  }

  return user_mentions;
}

std::vector<Url> BackEndHandler::_ComposeUrls(const std::vector<std::string> &urls) {
  std::vector<Url> target_urls;

  for (auto &url : urls) {
    Url target_url;
    target_url.expanded_url = url;
    target_url.shortened_url = HOSTNAME + GenRandomString(10);
    target_urls.push_back(target_url);
  }

  std::vector<nu::Future<void>> put_futures;
  for (auto &target_url : target_urls) {
    put_futures.emplace_back(_short_to_extended_map.put_async(
        target_url.shortened_url, target_url.expanded_url));
  }
  for (auto &put_future : put_futures) {
    put_future.get();
  }
  return target_urls;
}

void BackEndHandler::_WriteUserTimeline(int64_t post_id, int64_t user_id,
                                       int64_t timestamp) {
  _userid_to_timeline_map.apply(
      user_id,
      +[](std::pair<const int64_t, Timeline> &p, int64_t timestamp,
          int64_t post_id) { (p.second)[timestamp] = post_id; },
      timestamp, post_id);
}

std::vector<Post> BackEndHandler::_ReadUserTimeline(int64_t user_id, int start,
                                                    int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  auto post_ids = _userid_to_timeline_map.apply(
      user_id,
      +[](std::pair<const int64_t, Timeline> &p, int start, int stop) {
        auto start_iter = p.second.find_by_order(start);
        auto stop_iter = p.second.find_by_order(stop);
        std::vector<int64_t> post_ids;
        for (auto iter = start_iter; iter != stop_iter; iter++) {
          post_ids.push_back(iter->second);
        }
        return post_ids;
      },
      start, stop);
  return _ReadPosts(post_ids);
}

void BackEndHandler::_WriteHomeTimeline(
    int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id) {
  std::vector<nu::Future<void>> futures;

  auto future_constructor = [&](int64_t id) {
    return _userid_to_timeline_map.apply_async(
        id,
        +[](std::pair<const int64_t, Timeline> &p, int64_t timestamp,
            int64_t post_id) { (p.second)[timestamp] = post_id; },
        timestamp, post_id);
  };

  for (auto id : user_mentions_id) {
    futures.emplace_back(future_constructor(id));
  }

  auto follower_ids = _GetFollowers(user_id);
  for (auto id : follower_ids) {
    futures.emplace_back(future_constructor(id));
  }

  for (auto &future : futures) {
    future.get();
  }
}

std::vector<Post> BackEndHandler::_ReadHomeTimeline(int64_t user_id, int start,
                                                    int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  auto post_ids = _userid_to_timeline_map.apply(
      user_id,
      +[](std::pair<const int64_t, Timeline> &p, int start, int stop) {
        auto start_iter = p.second.find_by_order(start);
        auto stop_iter = p.second.find_by_order(stop);
        std::vector<int64_t> post_ids;
        for (auto iter = start_iter; iter != stop_iter; iter++) {
          post_ids.push_back(iter->second);
        }
        return post_ids;
      },
      start, stop);
  return _ReadPosts(post_ids);
}

std::vector<Post>
BackEndHandler::_ReadPosts(const std::vector<int64_t> &post_ids) {
  std::vector<nu::Future<std::optional<Post>>> post_futures;
  for (auto post_id : post_ids) {
    post_futures.emplace_back(_postid_to_post_map.get_async(post_id));
  }
  std::vector<Post> posts;
  for (auto &post_future : post_futures) {
    auto optional = post_future.get();
    BUG_ON(!optional);
    posts.emplace_back(*optional);
  }
  return posts;
}

void BackEndHandler::_Follow(int64_t user_id, int64_t followee_id) {
  auto add_followee_future = _userid_to_followees_map.apply_async(
      user_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t followee_id) {
        p.second.emplace(followee_id);
      },
      followee_id);
  auto add_follower_future = _userid_to_followers_map.apply_async(
      followee_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t user_id) {
        p.second.emplace(user_id);
      },
      user_id);
  add_followee_future.get();
  add_follower_future.get();
}

void BackEndHandler::_Unfollow(int64_t user_id, int64_t followee_id) {
  auto add_followee_future = _userid_to_followees_map.apply_async(
      user_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t followee_id) {
        p.second.erase(followee_id);
      },
      followee_id);
  auto add_follower_future = _userid_to_followers_map.apply_async(
      followee_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t user_id) {
        p.second.erase(user_id);
      },
      user_id);
  add_followee_future.get();
  add_follower_future.get();
}

std::vector<int64_t> BackEndHandler::_GetFollowers(int64_t user_id) {
  auto followers_set_optional = _userid_to_followers_map.get(user_id);
  auto followers_set = followers_set_optional
                           ? std::move(*followers_set_optional)
                           : std::set<int64_t>();
  return std::vector<int64_t>(followers_set.begin(), followers_set.end());
}

std::vector<int64_t> BackEndHandler::_GetFollowees(int64_t user_id) {
  auto followees_set_optional = _userid_to_followees_map.get(user_id);
  auto followees_set = followees_set_optional
                           ? std::move(*followees_set_optional)
                           : std::set<int64_t>();
  return std::vector<int64_t>(followees_set.begin(), followees_set.end());
}

void BackEndHandler::_FollowWithUsername(const std::string &_user_name,
                                         const std::string &_followee_name) {
  auto user_name = _user_name;
  auto followee_name = _followee_name;
  auto user_id_future = nu::async([&] { return _GetUserId(user_name); });
  auto followee_id_future =
      nu::async([&] { return _GetUserId(followee_name); });
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Follow(user_id, followee_id);
  }
}

void BackEndHandler::_UnfollowWithUsername(const std::string &_user_name,
                                           const std::string &_followee_name) {
  auto user_name = _user_name;
  auto followee_name = _followee_name;
  auto user_id_future = nu::async([&] { return _GetUserId(user_name); });
  auto followee_id_future =
      nu::async([&] { return _GetUserId(followee_name); });
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Unfollow(user_id, followee_id);
  }
}

int BackEndHandler::_GetCounter(int64_t timestamp) {
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

int64_t BackEndHandler::_ComposeUniqueId(PostType::type post_type) {
  _mutex.Lock();
  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count() -
      CUSTOM_EPOCH;
  int idx = _GetCounter(timestamp);
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
  std::string post_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t post_id = stoul(post_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;

  return post_id;
}

void BackEndHandler::_RegisterUserWithId(const std::string &first_name,
                                         const std::string &last_name,
                                         const std::string &username,
                                         const std::string &password,
                                         int64_t user_id) {
  UserProfile user_profile;
  user_profile.first_name = first_name;
  user_profile.last_name = last_name;
  user_profile.user_id = user_id;
  user_profile.salt = GenRandomString(32);
  user_profile.password_hashed =
      picosha2::hash256_hex_string(password + user_profile.salt);
  _username_to_userprofile_map.put(username, user_profile);
}

void BackEndHandler::_RegisterUser(const std::string &first_name,
                                   const std::string &last_name,
                                   const std::string &username,
                                   const std::string &password) {
  // Compose user_id
  _mutex.Lock();
  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count() -
      CUSTOM_EPOCH;
  int idx = _GetCounter(timestamp);
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

  RegisterUserWithId(first_name, last_name, username, password, user_id);
}

Creator
BackEndHandler::_ComposeCreatorWithUsername(const std::string &username) {
  auto user_id_optional = _username_to_userprofile_map.get(username);
  BUG_ON(!user_id_optional);
  return _ComposeCreatorWithUserId(user_id_optional->user_id, username);
}

Creator BackEndHandler::_ComposeCreatorWithUserId(int64_t user_id,
                                                  const std::string &username) {
  Creator creator;
  creator.username = username;
  creator.user_id = user_id;
  return creator;
}

std::variant<LoginErrorCode, std::string>
BackEndHandler::_Login(const std::string &username,
                       const std::string &password) {
  auto user_profile_optional = _username_to_userprofile_map.get(username);
  if (!user_profile_optional) {
    return NOT_REGISTERED;
  }
  auto &user_profile = *user_profile_optional;
  bool auth = (picosha2::hash256_hex_string(password + user_profile.salt) ==
               user_profile.password_hashed);
  if (!auth) {
    return WRONG_PASSWORD;
  }
  auto user_id_str = std::to_string(user_profile.user_id);
  auto timestamp_str =
      std::to_string(duration_cast<std::chrono::seconds>(
                         system_clock::now().time_since_epoch())
                         .count());
  jwt::jwt_object obj{algorithm("HS256"), secret(_secret),
                      payload({{"user_id", user_id_str},
                               {"username", username},
                               {"timestamp", timestamp_str},
                               {"ttl", "3600"}})};
  return obj.signature();
}

int64_t BackEndHandler::_GetUserId(const std::string &username) {
  auto user_id_optional = _username_to_userprofile_map.get(username);
  BUG_ON(!user_id_optional);
  return user_id_optional->user_id;
}

}  // namespace social_network

void do_work() {
  json config_json;

  BUG_ON(load_config_file("config/service-config.json", &config_json) != 0);
  int port = config_json["back-end-service"]["port"];

  std::shared_ptr<TServerSocket> server_socket =
      std::make_shared<TServerSocket>("0.0.0.0", port);

  auto back_end_handler = std::make_shared<BackEndHandler>();

  TThreadedServer server(std::make_shared<BackEndServiceProcessor>(
                             std::move(back_end_handler)),
                         server_socket,
                         std::make_shared<TFramedTransportFactory>(),
                         std::make_shared<TBinaryProtocolFactory>());
  std::cout << "Starting the back-end-service server ..." << std::endl;
  server.serve();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

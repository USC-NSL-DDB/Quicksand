#include <PicoSHA2/picosha2.h>
#include <chrono>
#include <iostream>
#include <jwt/jwt.hpp>
#include <regex>

#include "BackEndService.hpp"
#include "utils.hpp"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace jwt::params;

namespace social_network {

BackEndService::BackEndService(const std::string &machine_id,
                               const std::string &secret)
    : username_to_userprofile_map_(kHashTablePowerNumShards),
      filename_to_data_map_(kHashTablePowerNumShards),
      short_to_extended_map_(kHashTablePowerNumShards),
      userid_to_hometimeline_map_(kHashTablePowerNumShards),
      userid_to_usertimeline_map_(kHashTablePowerNumShards),
      postid_to_post_map_(kHashTablePowerNumShards),
      userid_to_followers_map_(kHashTablePowerNumShards),
      userid_to_followees_map_(kHashTablePowerNumShards),
      generator_(
          std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() %
                       0xffffffff)),
      distribution_(std::uniform_int_distribution<int>(0, 61)),
      machine_id_(machine_id), secret_(secret) {}

void BackEndService::ComposePost(const std::string &username, int64_t user_id,
                                 const std::string &text,
                                 const std::vector<int64_t> &_media_ids,
                                 const std::vector<std::string> &_media_types,
                                 const PostType::type post_type) {
  auto text_service_return_future =
      nu::async([&] { return ComposeText(text); });

  auto unique_id_future = nu::async([&] { return ComposeUniqueId(post_type); });

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

  auto creator = ComposeCreatorWithUserId(user_id, username);

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  auto unique_id = unique_id_future.get();
  auto write_user_timeline_future = nu::async(
      [&] { return WriteUserTimeline(unique_id, user_id, timestamp); });

  auto text_service_return = text_service_return_future.get();
  std::vector<int64_t> user_mention_ids;
  for (auto &item : text_service_return.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }
  auto write_home_timeline_future = nu::async([&] {
    return WriteHomeTimeline(unique_id, user_id, timestamp, user_mention_ids);
  });

  post.text = std::move(text_service_return.text);
  post.urls = std::move(text_service_return.urls);
  post.user_mentions = std::move(text_service_return.user_mentions);
  post.post_id = unique_id;
  post.media = std::move(medias);
  post.creator = std::move(creator);
  post.post_type = post_type;

  auto post_future =
      postid_to_post_map_.put_async(post.post_id, std::move(post));

  write_user_timeline_future.get();
  post_future.get();
  write_home_timeline_future.get();
}

TextServiceReturn BackEndService::ComposeText(const std::string &text) {
  auto http_pattern = "(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)";
  auto mention_pattern = "@[a-zA-Z0-9-_]+";

  std::vector<std::string> urls;
  std::smatch m;
  std::regex e(http_pattern);
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }
  auto target_urls_future = nu::async([&] { return ComposeUrls(urls); });

  std::vector<std::string> mention_usernames;
  e = mention_pattern;
  s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }

  auto user_mentions_future =
      nu::async([&] { return ComposeUserMentions(mention_usernames); });

  auto target_urls = target_urls_future.get();
  std::string updated_text;
  if (!urls.empty()) {
    s = text;
    e = http_pattern;
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

std::vector<UserMention>
BackEndService::ComposeUserMentions(const std::vector<std::string> &usernames) {
  std::vector<nu::Future<std::optional<UserProfile>>>
      user_profile_optional_futures;
  for (auto &username : usernames) {
    user_profile_optional_futures.emplace_back(
        username_to_userprofile_map_.get_async(username));
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

std::vector<Url>
BackEndService::ComposeUrls(const std::vector<std::string> &urls) {
  std::vector<Url> target_urls;

  for (auto &url : urls) {
    Url target_url;
    target_url.expanded_url = url;
    target_url.shortened_url = HOSTNAME + GenRandomString(10);
    target_urls.push_back(target_url);
  }

  std::vector<nu::Future<void>> put_futures;
  for (auto &target_url : target_urls) {
    put_futures.emplace_back(short_to_extended_map_.put_async(
        target_url.shortened_url, target_url.expanded_url));
  }
  for (auto &put_future : put_futures) {
    put_future.get();
  }
  return target_urls;
}

void BackEndService::WriteUserTimeline(int64_t post_id, int64_t user_id,
                                       int64_t timestamp) {
  userid_to_usertimeline_map_.apply(
      user_id,
      +[](std::pair<const int64_t, Timeline> &p, int64_t timestamp,
          int64_t post_id) { (p.second)[timestamp] = post_id; },
      timestamp, post_id);
}

std::vector<Post> BackEndService::ReadUserTimeline(int64_t user_id, int start,
                                                   int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  auto post_ids = userid_to_usertimeline_map_.apply(
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
  return ReadPosts(post_ids);
}

void BackEndService::WriteHomeTimeline(
    int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id) {
  std::vector<nu::Future<void>> futures;

  auto future_constructor = [&](int64_t id) {
    return userid_to_hometimeline_map_.apply_async(
        id,
        +[](std::pair<const int64_t, Timeline> &p, int64_t timestamp,
            int64_t post_id) { (p.second)[timestamp] = post_id; },
        timestamp, post_id);
  };

  for (auto id : user_mentions_id) {
    futures.emplace_back(future_constructor(id));
  }

  auto follower_ids = GetFollowers(user_id);
  for (auto id : follower_ids) {
    futures.emplace_back(future_constructor(id));
  }

  for (auto &future : futures) {
    future.get();
  }
}

std::vector<Post> BackEndService::ReadHomeTimeline(int64_t user_id, int start,
                                                   int stop) {
  if (stop <= start || start < 0) {
    return std::vector<Post>();
  }

  auto post_ids = userid_to_hometimeline_map_.apply(
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
  return ReadPosts(post_ids);
}

std::vector<Post>
BackEndService::ReadPosts(const std::vector<int64_t> &post_ids) {
  std::vector<nu::Future<std::optional<Post>>> post_futures;
  for (auto post_id : post_ids) {
    post_futures.emplace_back(postid_to_post_map_.get_async(post_id));
  }
  std::vector<Post> posts;
  for (auto &post_future : post_futures) {
    auto optional = post_future.get();
    BUG_ON(!optional);
    posts.emplace_back(*optional);
  }
  return posts;
}

void BackEndService::Follow(int64_t user_id, int64_t followee_id) {
  auto add_followee_future = userid_to_followees_map_.apply_async(
      user_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t followee_id) {
        p.second.emplace(followee_id);
      },
      followee_id);
  auto add_follower_future = userid_to_followers_map_.apply_async(
      followee_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t user_id) {
        p.second.emplace(user_id);
      },
      user_id);
  add_followee_future.get();
  add_follower_future.get();
}

void BackEndService::Unfollow(int64_t user_id, int64_t followee_id) {
  auto add_followee_future = userid_to_followees_map_.apply_async(
      user_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t followee_id) {
        p.second.erase(followee_id);
      },
      followee_id);
  auto add_follower_future = userid_to_followers_map_.apply_async(
      followee_id,
      +[](std::pair<const int64_t, std::set<int64_t>> &p, int64_t user_id) {
        p.second.erase(user_id);
      },
      user_id);
  add_followee_future.get();
  add_follower_future.get();
}

std::vector<int64_t> BackEndService::GetFollowers(int64_t user_id) {
  auto followers_set_optional = userid_to_followers_map_.get(user_id);
  auto followers_set = followers_set_optional
                           ? std::move(*followers_set_optional)
                           : std::set<int64_t>();
  return std::vector<int64_t>(followers_set.begin(), followers_set.end());
}

std::vector<int64_t> BackEndService::GetFollowees(int64_t user_id) {
  auto followees_set_optional = userid_to_followees_map_.get(user_id);
  auto followees_set = followees_set_optional
                           ? std::move(*followees_set_optional)
                           : std::set<int64_t>();
  return std::vector<int64_t>(followees_set.begin(), followees_set.end());
}

void BackEndService::FollowWithUsername(const std::string &_user_name,
                                        const std::string &_followee_name) {
  auto user_name = _user_name;
  auto followee_name = _followee_name;
  auto user_id_future = nu::async([&] { return GetUserId(user_name); });
  auto followee_id_future = nu::async([&] { return GetUserId(followee_name); });
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Follow(user_id, followee_id);
  }
}

void BackEndService::UnfollowWithUsername(const std::string &_user_name,
                                          const std::string &_followee_name) {
  auto user_name = _user_name;
  auto followee_name = _followee_name;
  auto user_id_future = nu::async([&] { return GetUserId(user_name); });
  auto followee_id_future = nu::async([&] { return GetUserId(followee_name); });
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Unfollow(user_id, followee_id);
  }
}

int BackEndService::GetCounter(int64_t timestamp) {
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

int64_t BackEndService::ComposeUniqueId(PostType::type post_type) {
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
  std::string post_id_str = machine_id_ + timestamp_hex + counter_hex;
  int64_t post_id = stoul(post_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;

  return post_id;
}

void BackEndService::RegisterUserWithId(const std::string &first_name,
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
  username_to_userprofile_map_.put(username, user_profile);
}

void BackEndService::RegisterUser(const std::string &first_name,
                                  const std::string &last_name,
                                  const std::string &username,
                                  const std::string &password) {
  // Compose user_id
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

  RegisterUserWithId(first_name, last_name, username, password, user_id);
}

Creator
BackEndService::ComposeCreatorWithUsername(const std::string &username) {
  auto user_id_optional = username_to_userprofile_map_.get(username);
  BUG_ON(!user_id_optional);
  return ComposeCreatorWithUserId(user_id_optional->user_id, username);
}

Creator BackEndService::ComposeCreatorWithUserId(int64_t user_id,
                                                 const std::string &username) {
  Creator creator;
  creator.username = username;
  creator.user_id = user_id;
  return creator;
}

std::variant<LoginErrorCode, std::string>
BackEndService::Login(const std::string &username,
                      const std::string &password) {
  auto user_profile_optional = username_to_userprofile_map_.get(username);
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
  jwt::jwt_object obj{algorithm("HS256"), secret(secret_),
                      payload({{"user_id", user_id_str},
                               {"username", username},
                               {"timestamp", timestamp_str},
                               {"ttl", "3600"}})};
  return obj.signature();
}

int64_t BackEndService::GetUserId(const std::string &username) {
  auto user_id_optional = username_to_userprofile_map_.get(username);
  BUG_ON(!user_id_optional);
  return user_id_optional->user_id;
}

void BackEndService::UploadMedia(const std::string &filename,
                                 const std::string &data) {
  filename_to_data_map_.put(filename, data);
}

std::string BackEndService::GetMedia(const std::string &filename) {
  auto optional = filename_to_data_map_.get(filename);
  BUG_ON(!optional);
  return *optional;
}

} // namespace social_network

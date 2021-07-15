#include <PicoSHA2/picosha2.h>
#include <iostream>

#include "BackEndService.hpp"
#include "utils.hpp"

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
      unique_id_generator_(machine_id),
      generator_(
          std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() %
                       0xffffffff)),
      distribution_(std::uniform_int_distribution<int>(0, 61)),
      secret_(secret) {}

void BackEndService::ComposePost(const std::string &username, int64_t user_id,
                                 const std::string &text,
                                 const std::vector<int64_t> &media_ids,
                                 const std::vector<std::string> &media_types,
                                 const PostType::type post_type) {
  auto text_service_return_future =
      nu::async([&] { return ComposeText(text); });

  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();

  auto unique_id = unique_id_generator_.Gen();
  auto write_user_timeline_future = nu::async(
      [&] { return WriteUserTimeline(unique_id, user_id, timestamp); });

  std::vector<Media> medias;
  BUG_ON(media_types.size() != media_ids.size());
  for (int i = 0; i < media_ids.size(); ++i) {
    Media media;
    media.media_id = media_ids[i];
    media.media_type = media_types[i];
    medias.emplace_back(media);
  }

  Post post;
  post.timestamp = timestamp;
  post.post_id = unique_id;
  post.media = std::move(medias);
  post.creator.username = username;
  post.creator.user_id = user_id;
  post.post_type = post_type;

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

  auto post_future =
      postid_to_post_map_.put_async(post.post_id, std::move(post));

  write_user_timeline_future.get();
  write_home_timeline_future.get();
  post_future.get();
}

TextServiceReturn BackEndService::ComposeText(const std::string &text) {
  auto target_urls_future =
      nu::async([&] { return ComposeUrls(MatchUrls(text)); });
  auto user_mentions_future =
      nu::async([&] { return ComposeUserMentions(MatchMentions(text)); });
  auto target_urls = target_urls_future.get();
  auto updated_text = ShortenUrlInText(text, target_urls);
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
    user_mention.username = usernames[i];
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
          int64_t post_id) {
        (p.second).insert(std::make_pair(timestamp, post_id));
      },
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
  auto follower_ids_future = nu::async([&] { return GetFollowers(user_id); });

  std::vector<nu::Future<void>> futures;

  auto future_constructor = [&](int64_t id) {
    return userid_to_hometimeline_map_.apply_async(
        id,
        +[](std::pair<const int64_t, Timeline> &p, int64_t timestamp,
            int64_t post_id) {
          (p.second).insert(std::make_pair(timestamp, post_id));
        },
        timestamp, post_id);
  };

  for (auto id : user_mentions_id) {
    futures.emplace_back(future_constructor(id));
  }

  auto follower_ids = follower_ids_future.get();
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
  auto followers_set = followers_set_optional.value_or(std::set<int64_t>());
  return std::vector<int64_t>(followers_set.begin(), followers_set.end());
}

std::vector<int64_t> BackEndService::GetFollowees(int64_t user_id) {
  auto followees_set_optional = userid_to_followees_map_.get(user_id);
  auto followees_set = followees_set_optional.value_or(std::set<int64_t>());
  return std::vector<int64_t>(followees_set.begin(), followees_set.end());
}

void BackEndService::FollowWithUsername(const std::string &user_name,
                                        const std::string &followee_name) {
  auto user_id_future = GetUserId(user_name);
  auto followee_id_future = GetUserId(followee_name);
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Follow(user_id, followee_id);
  }
}

void BackEndService::UnfollowWithUsername(const std::string &user_name,
                                          const std::string &followee_name) {
  auto user_id_future = GetUserId(user_name);
  auto followee_id_future = GetUserId(followee_name);
  auto user_id = user_id_future.get();
  auto followee_id = followee_id_future.get();
  if (user_id && followee_id) {
    Unfollow(user_id, followee_id);
  }
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
  RegisterUserWithId(first_name, last_name, username, password,
                     unique_id_generator_.Gen());
}

std::variant<LoginErrorCode, std::string>
BackEndService::Login(const std::string &username,
                      const std::string &password) {
  std::string signature;
  auto user_profile_optional = username_to_userprofile_map_.get(username);
  if (!user_profile_optional) {
    return NOT_REGISTERED;
  }
  if (VerifyLogin(signature, *user_profile_optional, username, password,
                  secret_)) {
    return signature;
  } else {
    return WRONG_PASSWORD;
  }
}

nu::Future<int64_t> BackEndService::GetUserId(const std::string &username) {
  return nu::async([&] {
    auto user_id_optional = username_to_userprofile_map_.get(username);
    BUG_ON(!user_id_optional);
    return user_id_optional->user_id;
  });
}

void BackEndService::UploadMedia(const std::string &filename,
                                 const std::string &data) {
  filename_to_data_map_.put(filename, data);
}

std::string BackEndService::GetMedia(const std::string &filename) {
  auto optional = filename_to_data_map_.get(filename);
  return optional.value_or("");
}

} // namespace social_network

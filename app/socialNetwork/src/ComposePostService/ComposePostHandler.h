#pragma once

#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>
#include <nu/rem_obj.hpp>

#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/social_network_types.h"
#include "HomeTimelineService.h"
#include "MediaService.h"
#include "PostStorageService.h"
#include "SocialGraphService.h"
#include "TextService.h"
#include "UniqueIdService.h"
#include "UserService.h"
#include "UserTimelineService.h"

namespace social_network {
using json = nlohmann::json;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class ComposePostHandler : public ComposePostServiceIf {
 public:
   ComposePostHandler();
   ~ComposePostHandler() override = default;

   // TODO: remove those helpers in the future.
   void ComposePost(int64_t req_id, const std::string &username,
                    int64_t user_id, const std::string &text,
                    const std::vector<int64_t> &media_ids,
                    const std::vector<std::string> &media_types,
                    PostType::type post_type) override;
   void StorePost(int64_t req_id, const Post &post) override;
   void ReadPost(Post &_return, int64_t req_id, int64_t post_id) override;
   void ReadPosts(std::vector<Post> &_return, int64_t req_id,
                  const std::vector<int64_t> &post_ids) override;
   void ReadUserTimeline(std::vector<Post> &, int64_t, int64_t, int,
                         int) override;
   void Login(std::string &_return, const int64_t req_id,
              const std::string &username,
              const std::string &password) override;
   void RegisterUser(const int64_t req_id, const std::string &first_name,
                     const std::string &last_name, const std::string &username,
                     const std::string &password) override;
   void RegisterUserWithId(const int64_t req_id, const std::string &first_name,
                           const std::string &last_name,
                           const std::string &username,
                           const std::string &password,
                           const int64_t user_id) override;
   void GetFollowers(std::vector<int64_t> &_return, const int64_t req_id,
                     const int64_t user_id) override;
   void Unfollow(const int64_t req_id, const int64_t user_id,
                 const int64_t followee_id) override;
   void UnfollowWithUsername(const int64_t req_id,
                             const std::string &user_usernmae,
                             const std::string &followee_username) override;
   void Follow(const int64_t req_id, const int64_t user_id,
               const int64_t followee_id) override;
   void FollowWithUsername(const int64_t req_id,
                           const std::string &user_usernmae,
                           const std::string &followee_username) override;
   void GetFollowees(std::vector<int64_t> &_return, const int64_t req_id,
                     const int64_t user_id) override;
   void ReadHomeTimeline(std::vector<Post> &_return, const int64_t req_id,
                         const int64_t user_id, const int32_t start,
                         const int32_t stop) override;

 private:
   nu::RemObj<TextService> _text_service_obj;
   nu::RemObj<UniqueIdService> _unique_id_service_obj;
   nu::RemObj<MediaService> _media_service_obj;
   nu::RemObj<PostStorageService> _post_storage_service_obj;
   nu::RemObj<UserTimelineService> _user_timeline_service_obj;
   nu::RemObj<UserService> _user_service_obj;
   nu::RemObj<SocialGraphService> _social_graph_service_obj;
   nu::RemObj<HomeTimelineService> _home_timeline_service_obj;
};

ComposePostHandler::ComposePostHandler() {
  _text_service_obj = nu::RemObj<TextService>::create();
  _unique_id_service_obj = nu::RemObj<UniqueIdService>::create();
  _media_service_obj = nu::RemObj<MediaService>::create();
  _post_storage_service_obj = nu::RemObj<PostStorageService>::create();
  _user_timeline_service_obj = nu::RemObj<UserTimelineService>::create(
      _post_storage_service_obj.get_cap());
  _user_service_obj = nu::RemObj<UserService>::create();
  _social_graph_service_obj =
      nu::RemObj<SocialGraphService>::create(_user_service_obj.get_cap());
  _home_timeline_service_obj = nu::RemObj<HomeTimelineService>::create(
      _post_storage_service_obj.get_cap(), _social_graph_service_obj.get_cap());
}

void ComposePostHandler::ComposePost(
    const int64_t req_id, const std::string &_username, int64_t user_id,
    const std::string &_text, const std::vector<int64_t> &_media_ids,
    const std::vector<std::string> &_media_types,
    const PostType::type post_type) {
  auto username = _username;
  auto creator_future =
      _user_service_obj.run_async(&UserService::ComposeCreatorWithUserId,
                                  req_id, user_id, std::move(username));

  auto text = _text;
  auto text_service_return_future = _text_service_obj.run_async(
      &TextService::ComposeText, req_id, std::move(text));

  auto unique_id_future = _unique_id_service_obj.run_async(
      &UniqueIdService::ComposeUniqueId, req_id, post_type);

  auto media_types = _media_types;
  auto media_ids = _media_ids;
  auto medias_future = _media_service_obj.run_async(
      &MediaService::ComposeMedia, req_id, std::move(media_types),
      std::move(media_ids));

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  auto text_service_return = std::move(text_service_return_future.get());
  post.text = text_service_return.text;
  post.urls = text_service_return.urls;
  post.user_mentions = text_service_return.user_mentions;
  post.post_id = unique_id_future.get();
  post.media = medias_future.get();
  post.creator = creator_future.get();
  post.req_id = req_id;
  post.post_type = post_type;

  std::vector<int64_t> user_mention_ids;
  for (auto &item : post.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }

  auto write_user_timeline_future = _user_timeline_service_obj.run_async(
      &UserTimelineService::WriteUserTimeline, req_id, post.post_id, user_id,
      timestamp);

  auto post_copy = post;
  auto post_future = _post_storage_service_obj.run_async(
      &PostStorageService::StorePost, req_id, std::move(post_copy));

  auto write_home_timeline_future = _home_timeline_service_obj.run_async(
      &HomeTimelineService::WriteHomeTimeline, req_id, post.post_id, user_id,
      timestamp, std::move(user_mention_ids));

  write_user_timeline_future.get();
  post_future.get();
  write_home_timeline_future.get();
}

void ComposePostHandler::StorePost(int64_t req_id, const Post &_post) {
  auto post = _post;
  _post_storage_service_obj.run(&PostStorageService::StorePost, req_id,
                                std::move(post));
}

void ComposePostHandler::ReadPost(Post &_return, int64_t req_id,
                                  int64_t post_id) {
  _return = _post_storage_service_obj.run(&PostStorageService::ReadPost, req_id,
                                          post_id);
}

void ComposePostHandler::ReadPosts(std::vector<Post> &_return, int64_t req_id,
                                   const std::vector<int64_t> &_post_ids) {
  auto post_ids = _post_ids;
  _return = _post_storage_service_obj.run(&PostStorageService::ReadPosts,
                                          req_id, std::move(post_ids));
}

void ComposePostHandler::ReadUserTimeline(std::vector<Post> &_return,
                                          int64_t req_id, int64_t user_id,
                                          int start, int stop) {
  _return = _user_timeline_service_obj.run(
      &UserTimelineService::ReadUserTimeline, req_id, user_id, start, stop);
}

void ComposePostHandler::Login(std::string &_return, const int64_t req_id,
                               const std::string &_username,
                               const std::string &_password) {
  auto username = _username;
  auto password = _password;
  auto variant = _user_service_obj.run(&UserService::Login, req_id,
                                  std::move(username), std::move(password));
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

void ComposePostHandler::RegisterUser(const int64_t req_id,
                                      const std::string &_first_name,
                                      const std::string &_last_name,
                                      const std::string &_username,
                                      const std::string &_password) {
  auto first_name = _first_name;
  auto last_name = _last_name;
  auto username = _username;
  auto password = _password;
  _user_service_obj.run(&UserService::RegisterUser, req_id,
                        std::move(first_name), std::move(last_name),
                        std::move(username), std::move(password));
}

void ComposePostHandler::RegisterUserWithId(const int64_t req_id,
                                            const std::string &_first_name,
                                            const std::string &_last_name,
                                            const std::string &_username,
                                            const std::string &_password,
                                            const int64_t user_id) {
  auto first_name = _first_name;
  auto last_name = _last_name;
  auto username = _username;
  auto password = _password;
  _user_service_obj.run(&UserService::RegisterUserWithId, req_id,
                        std::move(first_name), std::move(last_name),
                        std::move(username), std::move(password), user_id);
}

void ComposePostHandler::GetFollowers(std::vector<int64_t> &_return,
                                      const int64_t req_id,
                                      const int64_t user_id) {
  _return = _social_graph_service_obj.run(&SocialGraphService::GetFollowers,
                                          req_id, user_id);
}

void ComposePostHandler::Unfollow(const int64_t req_id, const int64_t user_id,
                                  const int64_t followee_id) {
  _social_graph_service_obj.run(&SocialGraphService::Unfollow, req_id, user_id,
                                followee_id);
}

void ComposePostHandler::UnfollowWithUsername(
    const int64_t req_id, const std::string &_user_username,
    const std::string &_followee_username) {
  auto user_username = _user_username;
  auto followee_username = _followee_username;
  _social_graph_service_obj.run(&SocialGraphService::UnfollowWithUsername,
                                req_id, std::move(user_username),
                                std::move(followee_username));
}

void ComposePostHandler::Follow(const int64_t req_id, const int64_t user_id,
                                const int64_t followee_id) {
  _social_graph_service_obj.run(&SocialGraphService::Follow, req_id, user_id,
                                followee_id);
}

void ComposePostHandler::FollowWithUsername(
    const int64_t req_id, const std::string &_user_username,
    const std::string &_followee_username) {
  auto user_username = _user_username;
  auto followee_username = _followee_username;
  _social_graph_service_obj.run(&SocialGraphService::FollowWithUsername, req_id,
                                std::move(user_username),
                                std::move(followee_username));
}

void ComposePostHandler::GetFollowees(std::vector<int64_t> &_return,
                                      const int64_t req_id,
                                      const int64_t user_id) {
  _return = _social_graph_service_obj.run(&SocialGraphService::GetFollowees,
                                          req_id, user_id);
}

void ComposePostHandler::ReadHomeTimeline(std::vector<Post> &_return,
                                          const int64_t req_id,
                                          const int64_t user_id,
                                          const int32_t start,
                                          const int32_t stop) {
  _return = _home_timeline_service_obj.run(
      &HomeTimelineService::ReadHomeTimeline, req_id, user_id, start, stop);
}

}  // namespace social_network

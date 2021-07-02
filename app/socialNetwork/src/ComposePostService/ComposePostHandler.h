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
#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
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
   ComposePostHandler(ClientPool<ThriftClient<HomeTimelineServiceClient>> *);
   ~ComposePostHandler() override = default;

   void ComposePost(int64_t req_id, const std::string &username,
                    int64_t user_id, const std::string &text,
                    const std::vector<int64_t> &media_ids,
                    const std::vector<std::string> &media_types,
                    PostType::type post_type) override;
   // TODO: remove those helpers in the future.
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

   // For compatibility purpose, To be removed in the future.
   bool _unique_id_pending_req = false;
   bool _unique_id_pending_resp = false;
   int64_t _unique_id_arg_req_id;
   PostType::type _unique_id_arg_post_type;
   int64_t _unique_id_resp;
   bool _media_pending_req = false;
   bool _media_pending_resp = false;
   int64_t _media_arg_req_id;
   std::vector<std::string> _media_arg_media_types;
   std::vector<int64_t> _media_arg_media_ids;
   std::vector<Media> _media_resp;
   bool _text_pending_req = false;
   bool _text_pending_resp = false;
   int64_t _text_arg_req_id;
   std::string _text_arg_text;
   TextServiceReturn _text_resp;
   bool _post_storage_storepost_pending_req = false;
   bool _post_storage_storepost_pending_resp = false;
   int64_t _post_storage_storepost_arg_req_id;
   Post _post_storage_storepost_arg_post;
   bool _post_storage_readpost_pending_req = false;
   bool _post_storage_readpost_pending_resp = false;
   int64_t _post_storage_readpost_arg_req_id;
   int64_t _post_storage_readpost_arg_post_id;
   Post _post_storage_readpost_resp;
   bool _post_storage_readposts_pending_req = false;
   bool _post_storage_readposts_pending_resp = false;
   int64_t _post_storage_readposts_arg_req_id;
   std::vector<int64_t> _post_storage_readposts_arg_post_ids;
   std::vector<Post> _post_storage_readposts_resp;
   bool _user_timeline_writeusertimeline_pending_req = false;
   bool _user_timeline_writeusertimeline_pending_resp = false;
   int64_t _user_timeline_writeusertimeline_arg_req_id;
   int64_t _user_timeline_writeusertimeline_arg_post_id;
   int64_t _user_timeline_writeusertimeline_arg_user_id;
   int64_t _user_timeline_writeusertimeline_arg_timestamp;
   bool _user_timeline_readusertimeline_pending_req = false;
   bool _user_timeline_readusertimeline_pending_resp = false;
   int64_t _user_timeline_readusertimeline_arg_req_id;
   int64_t _user_timeline_readusertimeline_arg_user_id;
   int64_t _user_timeline_readusertimeline_arg_start;
   int64_t _user_timeline_readusertimeline_arg_stop;
   std::vector<Post> _user_timeline_readusertimeline_resp;
   bool _user_login_pending_req = false;
   bool _user_login_pending_resp = false;
   int64_t _user_login_arg_req_id;
   std::string _user_login_arg_username;
   std::string _user_login_arg_password;
   std::variant<LoginErrorCode, std::string> _user_login_resp;
   bool _user_registeruser_pending_req = false;
   bool _user_registeruser_pending_resp = false;
   int64_t _user_registeruser_arg_req_id;
   std::string _user_registeruser_arg_first_name;
   std::string _user_registeruser_arg_last_name;
   std::string _user_registeruser_arg_username;
   std::string _user_registeruser_arg_password;
   bool _user_registeruserwithid_pending_req = false;
   bool _user_registeruserwithid_pending_resp = false;
   int64_t _user_registeruserwithid_arg_req_id;
   std::string _user_registeruserwithid_arg_first_name;
   std::string _user_registeruserwithid_arg_last_name;
   std::string _user_registeruserwithid_arg_username;
   std::string _user_registeruserwithid_arg_password;
   int64_t _user_registeruserwithid_arg_user_id;
   bool _social_graph_getfollowers_pending_req = false;
   bool _social_graph_getfollowers_pending_resp = false;
   int64_t _social_graph_getfollowers_arg_req_id;
   int64_t _social_graph_getfollowers_arg_user_id;
   std::vector<int64_t> _social_graph_getfollowers_resp;
   bool _social_graph_unfollow_pending_req = false;
   bool _social_graph_unfollow_pending_resp = false;
   int64_t _social_graph_unfollow_arg_req_id;
   int64_t _social_graph_unfollow_arg_user_id;
   int64_t _social_graph_unfollow_arg_followee_id;
   bool _social_graph_unfollowwithusername_pending_req = false;
   bool _social_graph_unfollowwithusername_pending_resp = false;
   int64_t _social_graph_unfollowwithusername_arg_req_id;
   std::string _social_graph_unfollowwithusername_arg_user_username;
   std::string _social_graph_unfollowwithusername_arg_followee_username;
   bool _social_graph_follow_pending_req = false;
   bool _social_graph_follow_pending_resp = false;
   int64_t _social_graph_follow_arg_req_id;
   int64_t _social_graph_follow_arg_user_id;
   int64_t _social_graph_follow_arg_followee_id;
   bool _social_graph_followwithusername_pending_req = false;
   bool _social_graph_followwithusername_pending_resp = false;
   int64_t _social_graph_followwithusername_arg_req_id;
   std::string _social_graph_followwithusername_arg_user_username;
   std::string _social_graph_followwithusername_arg_followee_username;
   bool _social_graph_getfollowees_pending_req = false;
   bool _social_graph_getfollowees_pending_resp = false;
   int64_t _social_graph_getfollowees_arg_req_id;
   int64_t _social_graph_getfollowees_arg_user_id;
   std::vector<int64_t> _social_graph_getfollowees_resp;
   bool _user_composecreator_pending_req = false;
   bool _user_composecreator_pending_resp = false;
   int64_t _user_composecreator_arg_req_id;
   int64_t _user_composecreator_arg_user_id;
   std::string _user_composecreator_arg_username;
   Creator _user_composecreator_resp;
   std::mutex _mutex_composepost;
   std::mutex _mutex_storepost;
   std::mutex _mutex_readpost;
   std::mutex _mutex_readposts;
   std::mutex _mutex_readusertimeline;
   std::mutex _mutex_login;
   std::mutex _mutex_registeruser;
   std::mutex _mutex_registeruserwithid;
   std::mutex _mutex_getfollowers;
   std::mutex _mutex_unfollow;
   std::mutex _mutex_unfollowwithusername;
   std::mutex _mutex_follow;
   std::mutex _mutex_followwithusername;
   std::mutex _mutex_getfollowees;
   void poller();

 private:
   ClientPool<ThriftClient<HomeTimelineServiceClient>>
       *_home_timeline_client_pool;

   nu::RemObj<TextService> _text_service_obj;
   nu::RemObj<UniqueIdService> _unique_id_service_obj;
   nu::RemObj<MediaService> _media_service_obj;
   nu::RemObj<PostStorageService> _post_storage_service_obj;
   nu::RemObj<UserTimelineService> _user_timeline_service_obj;
   nu::RemObj<UserService> _user_service_obj;
   nu::RemObj<SocialGraphService> _social_graph_service_obj;

   // TODO: remove those helpers in the future.
   void _StorePost(int64_t req_id, Post &&post);
   Post _ReadPost(int64_t req_id, int64_t post_id);
   std::vector<Post> _ReadPosts(int64_t req_id,
                                std::vector<int64_t> &&post_ids);
   std::vector<Post> _ReadUserTimeline(int64_t req_id, int64_t user_id,
                                       int start, int stop);

   void _UploadUserTimelineHelper(int64_t req_id, int64_t post_id,
                                  int64_t user_id, int64_t timestamp);

   void _UploadPostHelper(int64_t req_id, const Post &post);

   void _UploadHomeTimelineHelper(int64_t req_id, int64_t post_id,
                                  int64_t user_id, int64_t timestamp,
                                  const std::vector<int64_t> &user_mentions_id);

   Creator _ComposeCreaterHelper(int64_t req_id, int64_t user_id,
                                 const std::string &username);
   TextServiceReturn _ComposeTextHelper(int64_t req_id, std::string &&text);
   std::vector<Media>
   _ComposeMediaHelper(int64_t req_id, std::vector<std::string> &&media_types,
                       std::vector<int64_t> &&media_ids);
   int64_t _ComposeUniqueIdHelper(int64_t req_id, PostType::type post_type);
   void _Login(std::variant<LoginErrorCode, std::string> &_return,
               const int64_t req_id, const std::string &username,
               const std::string &password);
   void _RegisterUser(const int64_t req_id, const std::string &first_name,
                      const std::string &last_name, const std::string &username,
                      const std::string &password);
   void _RegisterUserWithId(const int64_t req_id, const std::string &first_name,
                            const std::string &last_name,
                            const std::string &username,
                            const std::string &password, const int64_t user_id);
   void _GetFollowers(std::vector<int64_t> &_return, const int64_t req_id,
                      const int64_t user_id);
   void _Unfollow(const int64_t req_id, const int64_t user_id,
                  const int64_t followee_id);
   void _UnfollowWithUsername(const int64_t req_id,
                              const std::string &user_username,
                              const std::string &followee_username);
   void _Follow(const int64_t req_id, const int64_t user_id,
                const int64_t followee_id);
   void _FollowWithUsername(const int64_t req_id,
                            const std::string &user_username,
                            const std::string &followee_username);
   void _GetFollowees(std::vector<int64_t> &_return, const int64_t req_id,
                      const int64_t user_id);
};

ComposePostHandler::ComposePostHandler(
    ClientPool<ThriftClient<HomeTimelineServiceClient>>
        *home_timeline_client_pool) {
  _home_timeline_client_pool = home_timeline_client_pool;
  // TODO: use the non-pinned variant.
  _text_service_obj = nu::RemObj<TextService>::create_pinned();
  _unique_id_service_obj = nu::RemObj<UniqueIdService>::create_pinned();
  _media_service_obj = nu::RemObj<MediaService>::create_pinned();
  _post_storage_service_obj = nu::RemObj<PostStorageService>::create_pinned();
  _user_timeline_service_obj = nu::RemObj<UserTimelineService>::create_pinned(
      _post_storage_service_obj.get_cap());
  _user_service_obj = nu::RemObj<UserService>::create_pinned();
  _social_graph_service_obj = nu::RemObj<SocialGraphService>::create_pinned(
      _user_service_obj.get_cap());
}

Creator
ComposePostHandler::_ComposeCreaterHelper(int64_t req_id, int64_t user_id,
                                          const std::string &_username) {
  auto username = _username;
  return _user_service_obj.run(&UserService::ComposeCreatorWithUserId, req_id,
                               user_id, std::move(username));
}

TextServiceReturn ComposePostHandler::_ComposeTextHelper(int64_t req_id,
                                                         std::string &&text) {
  return _text_service_obj.run(&TextService::ComposeText, req_id,
                               std::move(text));
}

std::vector<Media>
ComposePostHandler::_ComposeMediaHelper(int64_t req_id,
                                        std::vector<std::string> &&media_types,
                                        std::vector<int64_t> &&media_ids) {
  return _media_service_obj.run(&MediaService::ComposeMedia, req_id,
                                std::move(media_types), std::move(media_ids));
}

int64_t
ComposePostHandler::_ComposeUniqueIdHelper(int64_t req_id,
                                           const PostType::type post_type) {
  return _unique_id_service_obj.run(&UniqueIdService::ComposeUniqueId, req_id,
                                    post_type);
}

void ComposePostHandler::_UploadPostHelper(int64_t req_id, const Post &post) {
  StorePost(req_id, post);
}

void ComposePostHandler::_UploadUserTimelineHelper(int64_t req_id,
                                                   int64_t post_id,
                                                   int64_t user_id,
                                                   int64_t timestamp) {
  _user_timeline_service_obj.run(&UserTimelineService::WriteUserTimeline,
                                 req_id, post_id, user_id, timestamp);
}

void ComposePostHandler::_UploadHomeTimelineHelper(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id) {
  auto home_timeline_client_wrapper = _home_timeline_client_pool->Pop();
  if (!home_timeline_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to home-timeline-service";
    LOG(error) << se.message;
    throw se;
  }
  auto home_timeline_client = home_timeline_client_wrapper->GetClient();
  try {
    home_timeline_client->WriteHomeTimeline(req_id, post_id, user_id, timestamp,
                                            user_mentions_id);
  } catch (...) {
    _home_timeline_client_pool->Remove(home_timeline_client_wrapper);
    LOG(error) << "Failed to write home timeline to home-timeline-service";
    throw;
  }
  _home_timeline_client_pool->Keepalive(home_timeline_client_wrapper);
}

void ComposePostHandler::ComposePost(
    const int64_t req_id, const std::string &username, int64_t user_id,
    const std::string &text, const std::vector<int64_t> &media_ids,
    const std::vector<std::string> &media_types, const PostType::type post_type) {
  std::scoped_lock<std::mutex> lock(_mutex_composepost);

  _user_composecreator_arg_req_id = req_id;
  _user_composecreator_arg_user_id = user_id;
  _user_composecreator_arg_username = username;
  store_release(&_user_composecreator_pending_req, true);

  _text_arg_req_id = req_id;
  _text_arg_text = text;
  store_release(&_text_pending_req, true);

  _unique_id_arg_req_id = req_id;
  _unique_id_arg_post_type = post_type;
  store_release(&_unique_id_pending_req, true);

  _media_arg_req_id = req_id;
  _media_arg_media_types = media_types;
  _media_arg_media_ids = media_ids;
  store_release(&_media_pending_req, true);

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  while (!load_acquire(&_text_pending_resp))
    ;
  post.text = _text_resp.text;
  post.urls = _text_resp.urls;
  post.user_mentions = _text_resp.user_mentions;
  store_release(&_text_pending_resp, false);

  while (!load_acquire(&_unique_id_pending_resp))
    ;
  post.post_id = _unique_id_resp;
  store_release(&_unique_id_pending_resp, false);

  while (!load_acquire(&_media_pending_resp))
    ;
  post.media = _media_resp;
  store_release(&_media_pending_resp, false);

  while (!load_acquire(&_user_composecreator_pending_resp))
    ;
  post.creator = _user_composecreator_resp;
  store_release(&_user_composecreator_pending_resp, false);

  post.req_id = req_id;
  post.post_type = post_type;

  std::vector<int64_t> user_mention_ids;
  for (auto &item : post.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }

  _user_timeline_writeusertimeline_arg_req_id = req_id;
  _user_timeline_writeusertimeline_arg_post_id = post.post_id;
  _user_timeline_writeusertimeline_arg_user_id = user_id;
  _user_timeline_writeusertimeline_arg_timestamp = timestamp;
  store_release(&_user_timeline_writeusertimeline_pending_req, true);

  auto post_future =
      std::async(std::launch::async, &ComposePostHandler::_UploadPostHelper,
                 this, req_id, post);
  auto home_timeline_future = std::async(
      std::launch::async, &ComposePostHandler::_UploadHomeTimelineHelper, this,
      req_id, post.post_id, user_id, timestamp, user_mention_ids);

  while (!load_acquire(&_user_timeline_writeusertimeline_pending_resp))
    ;
  store_release(&_user_timeline_writeusertimeline_pending_resp, false);

  post_future.get();
  home_timeline_future.get();
}

void ComposePostHandler::_StorePost(int64_t req_id, Post &&post) {
  _post_storage_service_obj.run(&PostStorageService::StorePost, req_id,
                                std::move(post));
}

Post ComposePostHandler::_ReadPost(int64_t req_id, int64_t post_id) {
  return _post_storage_service_obj.run(&PostStorageService::ReadPost, req_id,
                                       post_id);
}

std::vector<Post>
ComposePostHandler::_ReadPosts(int64_t req_id,
                               std::vector<int64_t> &&post_ids) {
  return _post_storage_service_obj.run(&PostStorageService::ReadPosts, req_id,
                                       std::move(post_ids));
}

std::vector<Post> ComposePostHandler::_ReadUserTimeline(int64_t req_id,
                                                        int64_t user_id,
                                                        int start, int stop) {
  return _user_timeline_service_obj.run(&UserTimelineService::ReadUserTimeline,
                                        req_id, user_id, start, stop);
}

void ComposePostHandler::_Login(
    std::variant<LoginErrorCode, std::string> &_return, const int64_t req_id,
    const std::string &_username, const std::string &_password) {
  auto username = _username;
  auto password = _password;
  _return = _user_service_obj.run(&UserService::Login, req_id,
                                  std::move(username), std::move(password));
}

void ComposePostHandler::_RegisterUser(const int64_t req_id,
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

void ComposePostHandler::_RegisterUserWithId(const int64_t req_id,
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

void ComposePostHandler::_GetFollowers(std::vector<int64_t> &_return,
                                       const int64_t req_id,
                                       const int64_t user_id) {
  _return = _social_graph_service_obj.run(&SocialGraphService::GetFollowers,
                                          req_id, user_id);
}

void ComposePostHandler::_Unfollow(const int64_t req_id, const int64_t user_id,
				   const int64_t followee_id) {
  _social_graph_service_obj.run(&SocialGraphService::Unfollow, req_id, user_id,
                                followee_id);
}

void ComposePostHandler::_UnfollowWithUsername(
    const int64_t req_id, const std::string &_user_username,
    const std::string &_followee_username) {
  auto user_username = _user_username;
  auto followee_username = _followee_username;
  _social_graph_service_obj.run(&SocialGraphService::UnfollowWithUsername,
                                req_id, std::move(user_username),
                                std::move(followee_username));
}

void ComposePostHandler::_Follow(const int64_t req_id, const int64_t user_id,
                                 const int64_t followee_id) {
  _social_graph_service_obj.run(&SocialGraphService::Follow, req_id, user_id,
                                followee_id);
}

void ComposePostHandler::_FollowWithUsername(
    const int64_t req_id, const std::string &_user_username,
    const std::string &_followee_username) {
  auto user_username = _user_username;
  auto followee_username = _followee_username;
  _social_graph_service_obj.run(&SocialGraphService::FollowWithUsername, req_id,
                                std::move(user_username),
                                std::move(followee_username));
}

void ComposePostHandler::_GetFollowees(std::vector<int64_t> &_return,
                                       const int64_t req_id,
                                       const int64_t user_id) {
  _return = _social_graph_service_obj.run(&SocialGraphService::GetFollowees,
                                          req_id, user_id);
}

void ComposePostHandler::StorePost(int64_t req_id, const Post &post) {
  std::scoped_lock<std::mutex> lock(_mutex_storepost);

  _post_storage_storepost_arg_req_id = req_id;
  _post_storage_storepost_arg_post = post;
  store_release(&_post_storage_storepost_pending_req, true);

  while (!load_acquire(&_post_storage_storepost_pending_resp))
    ;
  store_release(&_post_storage_storepost_pending_resp, false);
}

void ComposePostHandler::ReadPost(Post &_return, int64_t req_id,
                                  int64_t post_id) {
  std::scoped_lock<std::mutex> lock(_mutex_readpost);

  _post_storage_readpost_arg_req_id = req_id;
  _post_storage_readpost_arg_post_id = post_id;
  store_release(&_post_storage_readpost_pending_req, true);

  while (!load_acquire(&_post_storage_readpost_pending_resp))
    ;
  _return = _post_storage_readpost_resp;
  store_release(&_post_storage_readpost_pending_resp, false);
}

void ComposePostHandler::ReadPosts(std::vector<Post> &_return, int64_t req_id,
                                   const std::vector<int64_t> &post_ids) {
  std::scoped_lock<std::mutex> lock(_mutex_readposts);

  _post_storage_readposts_arg_req_id = req_id;
  _post_storage_readposts_arg_post_ids = post_ids;
  store_release(&_post_storage_readposts_pending_req, true);

  while (!load_acquire(&_post_storage_readposts_pending_resp))
    ;
  _return = _post_storage_readposts_resp;
  store_release(&_post_storage_readposts_pending_resp, false);
}

void ComposePostHandler::ReadUserTimeline(std::vector<Post> &_return,
                                          int64_t req_id, int64_t user_id,
                                          int start, int stop) {
  std::scoped_lock<std::mutex> lock(_mutex_readusertimeline);

  _user_timeline_readusertimeline_arg_req_id = req_id;
  _user_timeline_readusertimeline_arg_user_id = user_id;
  _user_timeline_readusertimeline_arg_start = start;
  _user_timeline_readusertimeline_arg_stop = stop;
  store_release(&_user_timeline_readusertimeline_pending_req, true);

  while (!load_acquire(&_user_timeline_readusertimeline_pending_resp))
    ;
  _return = _user_timeline_readusertimeline_resp;
  store_release(&_user_timeline_readusertimeline_pending_resp, false);
}

void ComposePostHandler::Login(std::string &_return, const int64_t req_id,
                               const std::string &username,
                               const std::string &password) {
  // TODO: handle failure cases.
  std::scoped_lock<std::mutex> lock(_mutex_login);

  _user_login_arg_req_id = req_id;
  _user_login_arg_username = username;
  _user_login_arg_password = password;
  store_release(&_user_login_pending_req, true);

  while (!load_acquire(&_user_login_pending_resp))
    ;
  auto variant = _user_login_resp;
  store_release(&_user_login_pending_resp, false);

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
                                      const std::string &first_name,
                                      const std::string &last_name,
                                      const std::string &username,
                                      const std::string &password) {
  std::scoped_lock<std::mutex> lock(_mutex_registeruser);

  _user_registeruser_arg_req_id = req_id;
  _user_registeruser_arg_first_name = first_name;
  _user_registeruser_arg_last_name = last_name;
  _user_registeruser_arg_username = username;
  _user_registeruser_arg_password = password;
  store_release(&_user_registeruser_pending_req, true);

  while (!load_acquire(&_user_registeruser_pending_resp))
    ;
  store_release(&_user_registeruser_pending_resp, false);
}

void ComposePostHandler::RegisterUserWithId(const int64_t req_id,
                                            const std::string &first_name,
                                            const std::string &last_name,
                                            const std::string &username,
                                            const std::string &password,
                                            const int64_t user_id) {
  std::scoped_lock<std::mutex> lock(_mutex_registeruserwithid);

  _user_registeruserwithid_arg_req_id = req_id;
  _user_registeruserwithid_arg_first_name = first_name;
  _user_registeruserwithid_arg_last_name = last_name;
  _user_registeruserwithid_arg_username = username;
  _user_registeruserwithid_arg_password = password;
  _user_registeruserwithid_arg_user_id = user_id;
  store_release(&_user_registeruserwithid_pending_req, true);

  while (!load_acquire(&_user_registeruserwithid_pending_resp))
    ;
  store_release(&_user_registeruserwithid_pending_resp, false);
}

void ComposePostHandler::GetFollowers(std::vector<int64_t> &_return,
                                      const int64_t req_id,
                                      const int64_t user_id) {
  std::scoped_lock<std::mutex> lock(_mutex_getfollowers);

  _social_graph_getfollowers_arg_req_id = req_id;
  _social_graph_getfollowers_arg_user_id = user_id;
  store_release(&_social_graph_getfollowers_pending_req, true);

  while (!load_acquire(&_social_graph_getfollowers_pending_resp))
    ;
  _return = _social_graph_getfollowers_resp;
  store_release(&_social_graph_getfollowers_pending_resp, false);
}

void ComposePostHandler::Unfollow(const int64_t req_id, const int64_t user_id,
                                  const int64_t followee_id) {
  std::scoped_lock<std::mutex> lock(_mutex_unfollow);

  _social_graph_unfollow_arg_req_id = req_id;
  _social_graph_unfollow_arg_user_id = user_id;
  _social_graph_unfollow_arg_followee_id = followee_id;
  store_release(&_social_graph_unfollow_pending_req, true);

  while (!load_acquire(&_social_graph_unfollow_pending_resp))
    ;
  store_release(&_social_graph_unfollow_pending_resp, false);
}

void ComposePostHandler::UnfollowWithUsername(
    const int64_t req_id, const std::string &user_username,
    const std::string &followee_username) {
  std::scoped_lock<std::mutex> lock(_mutex_unfollowwithusername);

  _social_graph_unfollowwithusername_arg_req_id = req_id;
  _social_graph_unfollowwithusername_arg_user_username = user_username;
  _social_graph_unfollowwithusername_arg_followee_username = followee_username;
  store_release(&_social_graph_unfollowwithusername_pending_req, true);

  while (!load_acquire(&_social_graph_unfollowwithusername_pending_resp))
    ;
  store_release(&_social_graph_unfollowwithusername_pending_resp, false);
}

void ComposePostHandler::Follow(const int64_t req_id, const int64_t user_id,
                                const int64_t followee_id) {
  std::scoped_lock<std::mutex> lock(_mutex_follow);

  _social_graph_follow_arg_req_id = req_id;
  _social_graph_follow_arg_user_id = user_id;
  _social_graph_follow_arg_followee_id = followee_id;
  store_release(&_social_graph_follow_pending_req, true);

  while (!load_acquire(&_social_graph_follow_pending_resp))
    ;
  store_release(&_social_graph_follow_pending_resp, false);
}

void ComposePostHandler::FollowWithUsername(
    const int64_t req_id, const std::string &user_username,
    const std::string &followee_username) {
  std::scoped_lock<std::mutex> lock(_mutex_followwithusername);

  _social_graph_followwithusername_arg_req_id = req_id;
  _social_graph_followwithusername_arg_user_username = user_username;
  _social_graph_followwithusername_arg_followee_username = followee_username;
  store_release(&_social_graph_followwithusername_pending_req, true);

  while (!load_acquire(&_social_graph_followwithusername_pending_resp))
    ;
  store_release(&_social_graph_followwithusername_pending_resp, false);
}

void ComposePostHandler::GetFollowees(std::vector<int64_t> &_return,
                                      const int64_t req_id,
                                      const int64_t user_id) {
  std::scoped_lock<std::mutex> lock(_mutex_getfollowees);

  _social_graph_getfollowees_arg_req_id = req_id;
  _social_graph_getfollowees_arg_user_id = user_id;
  store_release(&_social_graph_getfollowees_pending_req, true);

  while (!load_acquire(&_social_graph_getfollowees_pending_resp))
    ;
  _return = _social_graph_getfollowees_resp;
  store_release(&_social_graph_getfollowees_pending_resp, false);
}

void ComposePostHandler::poller() {
  while (true) {
    if (load_acquire(&_unique_id_pending_req)) {
      _unique_id_pending_req = false;
      _unique_id_resp = _ComposeUniqueIdHelper(_unique_id_arg_req_id,
                                               _unique_id_arg_post_type);
      store_release(&_unique_id_pending_resp, true);
    }

    if (load_acquire(&_media_pending_req)) {
      _media_pending_req = false;
      _media_resp = _ComposeMediaHelper(_media_arg_req_id,
                                        std::move(_media_arg_media_types),
                                        std::move(_media_arg_media_ids));
      store_release(&_media_pending_resp, true);
    }

    if (load_acquire(&_text_pending_req)) {
      _text_pending_req = false;
      _text_resp =
          _ComposeTextHelper(_text_arg_req_id, std::move(_text_arg_text));
      store_release(&_text_pending_resp, true);
    }

    if (load_acquire(&_post_storage_storepost_pending_req)) {
      _post_storage_storepost_pending_req = false;
      _StorePost(_post_storage_storepost_arg_req_id,
                 std::move(_post_storage_storepost_arg_post));
      store_release(&_post_storage_storepost_pending_resp, true);
    }

    if (load_acquire(&_post_storage_readpost_pending_req)) {
      _post_storage_readpost_pending_req = false;
      _post_storage_readpost_resp =
          _ReadPost(_post_storage_readpost_arg_req_id,
                    _post_storage_readpost_arg_post_id);
      store_release(&_post_storage_readpost_pending_resp, true);
    }

    if (load_acquire(&_post_storage_readposts_pending_req)) {
      _post_storage_readposts_pending_req = false;
      _post_storage_readposts_resp =
          _ReadPosts(_post_storage_readposts_arg_req_id,
                     std::move(_post_storage_readposts_arg_post_ids));
      store_release(&_post_storage_readposts_pending_resp, true);
    }

    if (load_acquire(&_user_timeline_writeusertimeline_pending_req)) {
      _user_timeline_writeusertimeline_pending_req = false;
      _UploadUserTimelineHelper(_user_timeline_writeusertimeline_arg_req_id,
                                _user_timeline_writeusertimeline_arg_post_id,
                                _user_timeline_writeusertimeline_arg_user_id,
                                _user_timeline_writeusertimeline_arg_timestamp);
      store_release(&_user_timeline_writeusertimeline_pending_resp, true);
    }

    if (load_acquire(&_user_timeline_readusertimeline_pending_req)) {
      _user_timeline_readusertimeline_pending_req = false;
      _user_timeline_readusertimeline_resp =
          _ReadUserTimeline(_user_timeline_readusertimeline_arg_req_id,
                            _user_timeline_readusertimeline_arg_user_id,
                            _user_timeline_readusertimeline_arg_start,
                            _user_timeline_readusertimeline_arg_stop);
      store_release(&_user_timeline_readusertimeline_pending_resp, true);
    }

    if (load_acquire(&_user_login_pending_req)) {
      _user_login_pending_req = false;
      _Login(_user_login_resp, _user_login_arg_req_id, _user_login_arg_username,
             _user_login_arg_password);
      store_release(&_user_login_pending_resp, true);
    }

    if (load_acquire(&_user_registeruser_pending_req)) {
      _user_registeruser_pending_req = false;
      _RegisterUser(
          _user_registeruser_arg_req_id, _user_registeruser_arg_first_name,
          _user_registeruser_arg_last_name, _user_registeruser_arg_username,
          _user_registeruser_arg_password);
      store_release(&_user_registeruser_pending_resp, true);
    }

    if (load_acquire(&_user_registeruserwithid_pending_req)) {
      _user_registeruserwithid_pending_req = false;
      _RegisterUserWithId(_user_registeruserwithid_arg_req_id,
                          _user_registeruserwithid_arg_first_name,
                          _user_registeruserwithid_arg_last_name,
                          _user_registeruserwithid_arg_username,
                          _user_registeruserwithid_arg_password,
                          _user_registeruserwithid_arg_user_id);
      store_release(&_user_registeruserwithid_pending_resp, true);
    }

    if (load_acquire(&_social_graph_getfollowers_pending_req)) {
      _social_graph_getfollowers_pending_req = false;
      _GetFollowers(_social_graph_getfollowers_resp,
                    _social_graph_getfollowers_arg_req_id,
                    _social_graph_getfollowers_arg_user_id);
      store_release(&_social_graph_getfollowers_pending_resp, true);
    }

    if (load_acquire(&_social_graph_unfollow_pending_req)) {
      _social_graph_unfollow_pending_req = false;
      _Unfollow(_social_graph_unfollow_arg_req_id,
                _social_graph_unfollow_arg_user_id,
                _social_graph_unfollow_arg_user_id);
      store_release(&_social_graph_unfollow_pending_resp, true);
    }

    if (load_acquire(&_social_graph_unfollowwithusername_pending_req)) {
      _social_graph_unfollowwithusername_pending_req = false;
      _UnfollowWithUsername(
          _social_graph_unfollowwithusername_arg_req_id,
          _social_graph_unfollowwithusername_arg_user_username,
          _social_graph_unfollowwithusername_arg_followee_username);
      store_release(&_social_graph_unfollowwithusername_pending_resp, true);
    }

    if (load_acquire(&_social_graph_follow_pending_req)) {
      _social_graph_follow_pending_req = false;
      _Follow(_social_graph_follow_arg_req_id, _social_graph_follow_arg_user_id,
              _social_graph_follow_arg_followee_id);
      store_release(&_social_graph_follow_pending_resp, true);
    }

    if (load_acquire(&_social_graph_followwithusername_pending_req)) {
      _social_graph_followwithusername_pending_req = false;
      _FollowWithUsername(
          _social_graph_followwithusername_arg_req_id,
          _social_graph_followwithusername_arg_user_username,
          _social_graph_followwithusername_arg_followee_username);
      store_release(&_social_graph_followwithusername_pending_resp, true);
    }

    if (load_acquire(&_social_graph_getfollowees_pending_req)) {
      _social_graph_getfollowees_pending_req = false;
      _GetFollowees(_social_graph_getfollowees_resp,
                    _social_graph_getfollowees_arg_req_id,
                    _social_graph_getfollowees_arg_user_id);
      store_release(&_social_graph_getfollowees_pending_resp, true);
    }

    if (load_acquire(&_user_composecreator_pending_req)) {
      _user_composecreator_pending_req = false;
      _user_composecreator_resp = _ComposeCreaterHelper(
          _user_composecreator_arg_req_id, _user_composecreator_arg_user_id,
          _user_composecreator_arg_username);
      store_release(&_user_composecreator_pending_resp, true);
    }
  }
}

}  // namespace social_network

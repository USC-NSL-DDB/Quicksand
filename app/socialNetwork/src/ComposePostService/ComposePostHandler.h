#pragma once

#include <chrono>
#include <future>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <mutex>

#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <nu/rem_obj.hpp>

#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "MediaService.h"
#include "PostStorageService.h"
#include "TextService.h"
#include "UniqueIdService.h"
#include "UserTimelineService.h"

namespace social_network {
using json = nlohmann::json;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class ComposePostHandler : public ComposePostServiceIf {
 public:
   ComposePostHandler(ClientPool<ThriftClient<UserServiceClient>> *,
                      ClientPool<ThriftClient<HomeTimelineServiceClient>> *);
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
   std::mutex _mutex_composepost;
   std::mutex _mutex_storepost;
   std::mutex _mutex_readpost;
   std::mutex _mutex_readposts;
   std::mutex _mutex_readusertimeline;
   void poller();

 private:
  ClientPool<ThriftClient<UserServiceClient>> *_user_service_client_pool;
  ClientPool<ThriftClient<HomeTimelineServiceClient>>
      *_home_timeline_client_pool;

  nu::RemObj<TextService> _text_service_obj;
  nu::RemObj<UniqueIdService> _unique_id_service_obj;
  nu::RemObj<MediaService> _media_service_obj;
  nu::RemObj<PostStorageService> _post_storage_service_obj;
  nu::RemObj<UserTimelineService> _user_timeline_service_obj;

  // TODO: remove those helpers in the future.
  void _StorePost(int64_t req_id, Post &&post);
  Post _ReadPost(int64_t req_id, int64_t post_id);
  std::vector<Post> _ReadPosts(int64_t req_id, std::vector<int64_t> &&post_ids);
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
  _ComposeMediaHelper(int64_t req_id,
                      std::vector<std::string> &&media_types,
                      std::vector<int64_t> &&media_ids);
  int64_t _ComposeUniqueIdHelper(int64_t req_id, PostType::type post_type);
};

ComposePostHandler::ComposePostHandler(
    ClientPool<ThriftClient<UserServiceClient>> *user_service_client_pool,
    ClientPool<ThriftClient<HomeTimelineServiceClient>>
        *home_timeline_client_pool) {
  _user_service_client_pool = user_service_client_pool;
  _home_timeline_client_pool = home_timeline_client_pool;
  // TODO: use the non-pinned variant.
  _text_service_obj = nu::RemObj<TextService>::create_pinned();
  _unique_id_service_obj = nu::RemObj<UniqueIdService>::create_pinned();
  _media_service_obj = nu::RemObj<MediaService>::create_pinned();
  _post_storage_service_obj = nu::RemObj<PostStorageService>::create_pinned();
  _user_timeline_service_obj = nu::RemObj<UserTimelineService>::create_pinned(
      _post_storage_service_obj.get_cap());
}

Creator ComposePostHandler::_ComposeCreaterHelper(int64_t req_id,
                                                  int64_t user_id,
                                                  const std::string &username) {
  auto user_client_wrapper = _user_service_client_pool->Pop();
  if (!user_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    LOG(error) << se.message;
    throw se;
  }

  auto user_client = user_client_wrapper->GetClient();
  Creator _return_creator;
  try {
    user_client->ComposeCreatorWithUserId(_return_creator, req_id, user_id,
                                          username);
  } catch (...) {
    LOG(error) << "Failed to send compose-creator to user-service";
    _user_service_client_pool->Remove(user_client_wrapper);
    throw;
  }
  _user_service_client_pool->Keepalive(user_client_wrapper);
  return _return_creator;
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

  auto creator_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeCreaterHelper,
                 this, req_id, user_id, username);

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

  post.creator = creator_future.get();
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
      _UploadUserTimelineHelper(
          _user_timeline_writeusertimeline_arg_req_id, _user_timeline_writeusertimeline_arg_post_id,
          _user_timeline_writeusertimeline_arg_user_id, _user_timeline_writeusertimeline_arg_timestamp);
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
  }
}

}  // namespace social_network

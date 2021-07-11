#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <nu/dis_hash_table.hpp>
#include <nu/rem_obj.hpp>
#include <nu/runtime.hpp>
#include <regex>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <vector>

#include "../gen-cpp/BackEndService.h"
#include "../gen-cpp/social_network_types.h"
#include "HomeTimelineService.h"
#include "PostStorageService.h"
#include "SocialGraphService.h"
#include "UniqueIdService.h"
#include "UrlShortenService.h"
#include "UserService.h"
#include "UserTimelineService.h"
#include "UserMentionService.h"
#include "utils.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

namespace social_network {

using json = nlohmann::json;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

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
  UserService::UserProfileMap _username_to_userprofile_map;
  nu::DistributedHashTable<std::string, std::string> _filename_to_data_map;

  nu::RemObj<UniqueIdService> _unique_id_service_obj;
  nu::RemObj<PostStorageService> _post_storage_service_obj;
  nu::RemObj<UserTimelineService> _user_timeline_service_obj;
  nu::RemObj<UserService> _user_service_obj;
  nu::RemObj<SocialGraphService> _social_graph_service_obj;
  nu::RemObj<HomeTimelineService> _home_timeline_service_obj;
  nu::RemObj<UrlShortenService> _url_shorten_service_obj;
  nu::RemObj<UserMentionService> _user_mention_service_obj;

  TextServiceReturn ComposeText(const std::string &text);
};

BackEndHandler::BackEndHandler()
    : _username_to_userprofile_map(
          UserService::kDefaultHashTablePowerNumShards) {
  _unique_id_service_obj = nu::RemObj<UniqueIdService>::create();
  _post_storage_service_obj = nu::RemObj<PostStorageService>::create();
  _user_timeline_service_obj = nu::RemObj<UserTimelineService>::create(
      _post_storage_service_obj.get_cap());
  _user_service_obj =
      nu::RemObj<UserService>::create(_username_to_userprofile_map.get_cap());
  _social_graph_service_obj =
      nu::RemObj<SocialGraphService>::create(_user_service_obj.get_cap());
  _home_timeline_service_obj = nu::RemObj<HomeTimelineService>::create(
      _post_storage_service_obj.get_cap(), _social_graph_service_obj.get_cap());
  _url_shorten_service_obj = nu::RemObj<UrlShortenService>::create();
  _user_mention_service_obj = nu::RemObj<UserMentionService>::create(
      _username_to_userprofile_map.get_cap());
}

void BackEndHandler::ComposePost(const std::string &_username, int64_t user_id,
                                 const std::string &text,
                                 const std::vector<int64_t> &_media_ids,
                                 const std::vector<std::string> &_media_types,
                                 const PostType::type post_type) {
  auto text_service_return_future =
      nu::Promise<TextServiceReturn>::create([&]() {
        return ComposeText(text);
      })->get_future();

  auto unique_id_future = _unique_id_service_obj.run_async(
      &UniqueIdService::ComposeUniqueId, post_type);

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
  auto creator_future = _user_service_obj.run_async(
      &UserService::ComposeCreatorWithUserId, user_id, std::move(username));

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  auto unique_id = unique_id_future.get();
  auto write_user_timeline_future = _user_timeline_service_obj.run_async(
      &UserTimelineService::WriteUserTimeline, unique_id, user_id, timestamp);

  auto text_service_return = text_service_return_future.get();
  std::vector<int64_t> user_mention_ids;
  for (auto &item : text_service_return.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }
  auto write_home_timeline_future = _home_timeline_service_obj.run_async(
      &HomeTimelineService::WriteHomeTimeline, unique_id, user_id, timestamp,
      std::move(user_mention_ids));

  post.text = std::move(text_service_return.text);
  post.urls = std::move(text_service_return.urls);
  post.user_mentions = std::move(text_service_return.user_mentions);
  post.post_id = unique_id;
  post.media = std::move(medias);
  post.creator = creator_future.get();
  post.post_type = post_type;

  auto post_copy = post;
  auto post_future = _post_storage_service_obj.run_async(
      &PostStorageService::StorePost, std::move(post_copy));

  write_user_timeline_future.get();
  post_future.get();
  write_home_timeline_future.get();
}

void BackEndHandler::ReadUserTimeline(std::vector<Post> &_return,
                                      int64_t user_id, int start, int stop) {
  _return = _user_timeline_service_obj.run(
      &UserTimelineService::ReadUserTimeline, user_id, start, stop);
}

void BackEndHandler::Login(std::string &_return, const std::string &_username,
                           const std::string &_password) {
  auto username = _username;
  auto password = _password;
  auto variant = _user_service_obj.run(&UserService::Login, std::move(username),
                                       std::move(password));
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

void BackEndHandler::RegisterUser(const std::string &_first_name,
                                  const std::string &_last_name,
                                  const std::string &_username,
                                  const std::string &_password) {
  auto first_name = _first_name;
  auto last_name = _last_name;
  auto username = _username;
  auto password = _password;
  _user_service_obj.run(&UserService::RegisterUser, std::move(first_name),
                        std::move(last_name), std::move(username),
                        std::move(password));
}

void BackEndHandler::RegisterUserWithId(const std::string &_first_name,
                                        const std::string &_last_name,
                                        const std::string &_username,
                                        const std::string &_password,
                                        const int64_t user_id) {
  auto first_name = _first_name;
  auto last_name = _last_name;
  auto username = _username;
  auto password = _password;
  _user_service_obj.run(&UserService::RegisterUserWithId, std::move(first_name),
                        std::move(last_name), std::move(username),
                        std::move(password), user_id);
}

void BackEndHandler::GetFollowers(std::vector<int64_t> &_return,
                                  const int64_t user_id) {
  _return =
      _social_graph_service_obj.run(&SocialGraphService::GetFollowers, user_id);
}

void BackEndHandler::Unfollow(const int64_t user_id,
                              const int64_t followee_id) {
  _social_graph_service_obj.run(&SocialGraphService::Unfollow, user_id,
                                followee_id);
}

void BackEndHandler::UnfollowWithUsername(const std::string &_user_username,
    const std::string &_followee_username) {
  auto user_username = _user_username;
  auto followee_username = _followee_username;
  _social_graph_service_obj.run(&SocialGraphService::UnfollowWithUsername,
                                std::move(user_username),
                                std::move(followee_username));
}

void BackEndHandler::Follow(const int64_t user_id, const int64_t followee_id) {
  _social_graph_service_obj.run(&SocialGraphService::Follow, user_id,
                                followee_id);
}

void BackEndHandler::FollowWithUsername(const std::string &_user_username,
                                        const std::string &_followee_username) {
  auto user_username = _user_username;
  auto followee_username = _followee_username;
  _social_graph_service_obj.run(&SocialGraphService::FollowWithUsername,
                                std::move(user_username),
                                std::move(followee_username));
}

void BackEndHandler::GetFollowees(std::vector<int64_t> &_return,
                                  const int64_t user_id) {
  _return =
      _social_graph_service_obj.run(&SocialGraphService::GetFollowees, user_id);
}

void BackEndHandler::ReadHomeTimeline(std::vector<Post> &_return,
                                      const int64_t user_id,
                                      const int32_t start, const int32_t stop) {
  _return = _home_timeline_service_obj.run(
      &HomeTimelineService::ReadHomeTimeline, user_id, start, stop);
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

TextServiceReturn BackEndHandler::ComposeText(const std::string &text) {
  std::vector<std::string> urls;
  std::smatch m;
  std::regex e("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }
  auto target_urls_future =
      _url_shorten_service_obj.run_async(&UrlShortenService::ComposeUrls, urls);

  std::vector<std::string> mention_usernames;
  e = "@[a-zA-Z0-9-_]+";
  s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }
  auto user_mentions_future = _user_mention_service_obj.run_async(
      &UserMentionService::ComposeUserMentions, std::move(mention_usernames));

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

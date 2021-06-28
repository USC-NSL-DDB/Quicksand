#include <runtime.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/FrontEndProxy.h"
#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/SocialGraphService.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../utils.h"
#include "../utils_thrift.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

namespace social_network {

class FrontEndProxyHandler : public FrontEndProxyIf {
public:
  FrontEndProxyHandler(
      ClientPool<ThriftClient<HomeTimelineServiceClient>>
          *home_timeline_client_pool,
      ClientPool<ThriftClient<UserServiceClient>> *user_client_pool,
      ClientPool<ThriftClient<UserTimelineServiceClient>>
          *user_timeline_client_pool,
      ClientPool<ThriftClient<SocialGraphServiceClient>>
          *social_graph_client_pool,
      ClientPool<ThriftClient<ComposePostServiceClient>>
          *compose_post_client_pool)
      : home_timeline_client_pool_(home_timeline_client_pool),
        user_client_pool_(user_client_pool),
        user_timeline_client_pool_(user_timeline_client_pool),
        social_graph_client_pool_(social_graph_client_pool),
        compose_post_client_pool_(compose_post_client_pool) {}

  void ReadHomeTimeline(std::vector<Post> &_return, const int64_t req_id,
                        const int64_t user_id, const int32_t start,
                        const int32_t stop) {
    auto home_timeline_client_wrapper = home_timeline_client_pool_->Pop();
    if (!home_timeline_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to home-timeline-service";
      throw se;
    }
    auto home_timeline_client = home_timeline_client_wrapper->GetClient();
    try {
      home_timeline_client->ReadHomeTimeline(_return, req_id, user_id, start,
                                             stop);
    } catch (...) {
      home_timeline_client_pool_->Remove(home_timeline_client_wrapper);
      LOG(error) << "Failed to read posts from home-timeline-service";
      throw;
    }
    home_timeline_client_pool_->Keepalive(home_timeline_client_wrapper);
  }

  void ComposePost(const int64_t req_id, const std::string &username,
                   const int64_t user_id, const std::string &text,
                   const std::vector<int64_t> &media_ids,
                   const std::vector<std::string> &media_types,
                   const PostType::type post_type) {
    auto compose_post_client_wrapper = compose_post_client_pool_->Pop();
    if (!compose_post_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to compose-post-service";
      throw se;
    }
    auto compose_post_client = compose_post_client_wrapper->GetClient();
    try {
      compose_post_client->ComposePost(req_id, username, user_id, text,
                                       media_ids, media_types, post_type);
    } catch (...) {
      compose_post_client_pool_->Remove(compose_post_client_wrapper);
      LOG(error) << "Failed to read posts from compose-post-service";
      throw;
    }
    compose_post_client_pool_->Keepalive(compose_post_client_wrapper);
  }

  void GetFollowers(std::vector<int64_t> &_return, const int64_t req_id,
                    const int64_t user_id) {
    auto social_graph_client_wrapper = social_graph_client_pool_->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->GetFollowers(_return, req_id, user_id);
    } catch (...) {
      social_graph_client_pool_->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to read posts from social-graph-service";
      throw;
    }
    social_graph_client_pool_->Keepalive(social_graph_client_wrapper);
  }

  void Unfollow(const int64_t req_id, const int64_t user_id,
                const int64_t followee_id) {
    auto social_graph_client_wrapper = social_graph_client_pool_->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->Unfollow(req_id, user_id, followee_id);
    } catch (...) {
      social_graph_client_pool_->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to read posts from social-graph-service";
      throw;
    }
    social_graph_client_pool_->Keepalive(social_graph_client_wrapper);
  }

  void UnfollowWithUsername(const int64_t req_id,
                            const std::string &user_usernmae,
                            const std::string &followee_username) {
    auto social_graph_client_wrapper = social_graph_client_pool_->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->UnfollowWithUsername(req_id, user_usernmae,
                                                followee_username);
    } catch (...) {
      social_graph_client_pool_->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to read posts from social-graph-service";
      throw;
    }
    social_graph_client_pool_->Keepalive(social_graph_client_wrapper);
  }

  void Login(std::string &_return, const int64_t req_id,
             const std::string &username, const std::string &password) {
    auto user_client_wrapper = user_client_pool_->Pop();
    if (!user_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to user-service";
      throw se;
    }
    auto user_client = user_client_wrapper->GetClient();
    try {
      user_client->Login(_return, req_id, username, password);
    } catch (...) {
      user_client_pool_->Remove(user_client_wrapper);
      LOG(error) << "Failed to read posts from user-service";
      throw;
    }
    user_client_pool_->Keepalive(user_client_wrapper);
  }

  void Follow(const int64_t req_id, const int64_t user_id,
              const int64_t followee_id) {
    auto social_graph_client_wrapper = social_graph_client_pool_->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->Follow(req_id, user_id, followee_id);
    } catch (...) {
      social_graph_client_pool_->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to read posts from social-graph-service";
      throw;
    }
    social_graph_client_pool_->Keepalive(social_graph_client_wrapper);
  }

  void FollowWithUsername(const int64_t req_id,
                          const std::string &user_usernmae,
                          const std::string &followee_username) {
    auto social_graph_client_wrapper = social_graph_client_pool_->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->FollowWithUsername(req_id, user_usernmae,
                                              followee_username);
    } catch (...) {
      social_graph_client_pool_->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to read posts from social-graph-service";
      throw;
    }
    social_graph_client_pool_->Keepalive(social_graph_client_wrapper);
  }

  void RegisterUser(const int64_t req_id, const std::string &first_name,
                    const std::string &last_name, const std::string &username,
                    const std::string &password) {
    auto user_client_wrapper = user_client_pool_->Pop();
    if (!user_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to user-service";
      throw se;
    }
    auto user_client = user_client_wrapper->GetClient();
    try {
      user_client->RegisterUser(req_id, first_name, last_name, username,
                                password);
    } catch (...) {
      user_client_pool_->Remove(user_client_wrapper);
      LOG(error) << "Failed to read posts from user-service";
      throw;
    }
    user_client_pool_->Keepalive(user_client_wrapper);
  }

  void GetFollowees(std::vector<int64_t> &_return, const int64_t req_id,
                    const int64_t user_id) {
    auto social_graph_client_wrapper = social_graph_client_pool_->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->GetFollowees(_return, req_id, user_id);
    } catch (...) {
      social_graph_client_pool_->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to read posts from social-graph-service";
      throw;
    }
    social_graph_client_pool_->Keepalive(social_graph_client_wrapper);
  }

  void ReadUserTimeline(std::vector<Post> &_return, const int64_t req_id,
                        const int64_t user_id, const int32_t start,
                        const int32_t stop) {
    auto user_timeline_client_wrapper = user_timeline_client_pool_->Pop();
    if (!user_timeline_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to user-timeline-service";
      throw se;
    }
    auto user_timeline_client = user_timeline_client_wrapper->GetClient();
    try {
      user_timeline_client->ReadUserTimeline(_return, req_id, user_id, start,
                                             stop);
    } catch (...) {
      user_timeline_client_pool_->Remove(user_timeline_client_wrapper);
      LOG(error) << "Failed to read posts from user-timeline-service";
      throw;
    }
    user_timeline_client_pool_->Keepalive(user_timeline_client_wrapper);
  }

  void RegisterUserWithId(const int64_t req_id, const std::string &first_name,
                          const std::string &last_name,
                          const std::string &username,
                          const std::string &password, const int64_t user_id) {
    auto user_client_wrapper = user_client_pool_->Pop();
    if (!user_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to user-service";
      throw se;
    }
    auto user_client = user_client_wrapper->GetClient();
    try {
      user_client->RegisterUserWithId(req_id, first_name, last_name, username,
                                      password, user_id);
    } catch (...) {
      user_client_pool_->Remove(user_client_wrapper);
      LOG(error) << "Failed to read posts from user-service";
      throw;
    }
    user_client_pool_->Keepalive(user_client_wrapper);
  }

private:
  ClientPool<ThriftClient<HomeTimelineServiceClient>>
      *home_timeline_client_pool_;
  ClientPool<ThriftClient<UserServiceClient>> *user_client_pool_;
  ClientPool<ThriftClient<UserTimelineServiceClient>>
      *user_timeline_client_pool_;
  ClientPool<ThriftClient<SocialGraphServiceClient>> *social_graph_client_pool_;
  ClientPool<ThriftClient<ComposePostServiceClient>> *compose_post_client_pool_;
};

} // namespace social_network

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

void do_work() {
  signal(SIGINT, sigintHandler);
  init_logger();

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) == 0) {
    int port = config_json["front-end-proxy"]["port"];

    int home_timeline_port = config_json["home-timeline-service"]["port"];
    std::string home_timeline_addr =
        config_json["home-timeline-service"]["addr"];
    int home_timeline_conns =
        config_json["home-timeline-service"]["connections"];
    int home_timeline_timeout =
        config_json["home-timeline-service"]["timeout_ms"];
    int home_timeline_keepalive =
        config_json["home-timeline-service"]["keepalive_ms"];

    int user_port = config_json["user-service"]["port"];
    std::string user_addr = config_json["user-service"]["addr"];
    int user_conns = config_json["user-service"]["connections"];
    int user_timeout = config_json["user-service"]["timeout_ms"];
    int user_keepalive = config_json["user-service"]["keepalive_ms"];

    int user_timeline_port = config_json["user-timeline-service"]["port"];
    std::string user_timeline_addr =
        config_json["user-timeline-service"]["addr"];
    int user_timeline_conns =
        config_json["user-timeline-service"]["connections"];
    int user_timeline_timeout =
        config_json["user-timeline-service"]["timeout_ms"];
    int user_timeline_keepalive =
        config_json["user-timeline-service"]["keepalive_ms"];

    std::string social_graph_service_addr =
        config_json["social-graph-service"]["addr"];
    int social_graph_service_port = config_json["social-graph-service"]["port"];
    int social_graph_service_conns =
        config_json["social-graph-service"]["connections"];
    int social_graph_service_timeout =
        config_json["social-graph-service"]["timeout_ms"];
    int social_graph_service_keepalive =
        config_json["social-graph-service"]["keepalive_ms"];

    std::string compose_post_service_addr =
        config_json["compose-post-service"]["addr"];
    int compose_post_service_port = config_json["compose-post-service"]["port"];
    int compose_post_service_conns =
        config_json["compose-post-service"]["connections"];
    int compose_post_service_timeout =
        config_json["compose-post-service"]["timeout_ms"];
    int compose_post_service_keepalive =
        config_json["compose-post-service"]["keepalive_ms"];

    ClientPool<ThriftClient<HomeTimelineServiceClient>>
        home_timeline_client_pool("home-timeline-service-client",
                                  home_timeline_addr, home_timeline_port, 0,
                                  home_timeline_conns, home_timeline_timeout,
                                  home_timeline_keepalive, config_json);

    ClientPool<ThriftClient<UserServiceClient>> user_client_pool(
        "user-service-client", user_addr, user_port, 0, user_conns,
        user_timeout, user_keepalive, config_json);

    ClientPool<ThriftClient<UserTimelineServiceClient>>
        user_timeline_client_pool("user-timeline-client", user_timeline_addr,
                                  user_timeline_port, 0, user_timeline_conns,
                                  user_timeline_timeout,
                                  user_timeline_keepalive, config_json);

    ClientPool<ThriftClient<SocialGraphServiceClient>> social_graph_client_pool(
        "social-graph-service", social_graph_service_addr,
        social_graph_service_port, 0, social_graph_service_conns,
        social_graph_service_timeout, social_graph_service_keepalive,
        config_json);

    ClientPool<ThriftClient<ComposePostServiceClient>> compose_post_client_pool(
        "compose-post-service", compose_post_service_addr,
        compose_post_service_port, 0, compose_post_service_conns,
        compose_post_service_timeout, compose_post_service_keepalive,
        config_json);

    std::shared_ptr<TServerSocket> server_socket =
        get_server_socket(config_json, "0.0.0.0", port);
    TThreadedServer server(
        std::make_shared<FrontEndProxyProcessor>(
            std::make_shared<FrontEndProxyHandler>(
                &home_timeline_client_pool, &user_client_pool,
                &user_timeline_client_pool, &social_graph_client_pool,
                &compose_post_client_pool)),
        server_socket, std::make_shared<TFramedTransportFactory>(),
        std::make_shared<TBinaryProtocolFactory>());
    LOG(info) << "Starting the front-end-proxy server...";
    server.serve();
  } else {
    LOG(error) << "Failed to load the config json file.";
  }
}

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] { do_work(); });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <nlohmann/json.hpp>

#include "../ClientPool.h"
#include "../logger.h"
#include "../utils.h"
#include "../utils_redis.h"
#include "../utils_thrift.h"
#include "HomeTimelineHandler.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["home-timeline-service"]["port"];

  int compose_post_port = config_json["compose-post-service"]["port"];
  std::string compose_post_addr = config_json["compose-post-service"]["addr"];
  int compose_post_conns = config_json["compose-post-service"]["connections"];
  int compose_post_timeout = config_json["compose-post-service"]["timeout_ms"];
  int compose_post_keepalive =
      config_json["compose-post-service"]["keepalive_ms"];

  int social_graph_port = config_json["social-graph-service"]["port"];
  std::string social_graph_addr = config_json["social-graph-service"]["addr"];
  int social_graph_conns = config_json["social-graph-service"]["connections"];
  int social_graph_timeout = config_json["social-graph-service"]["timeout_ms"];
  int social_graph_keepalive =
      config_json["social-graph-service"]["keepalive_ms"];

  ClientPool<ThriftClient<ComposePostServiceClient>> compose_post_client_pool(
      "compose-post-client", compose_post_addr, compose_post_port, 0,
      compose_post_conns, compose_post_timeout, compose_post_keepalive,
      config_json);

  ClientPool<ThriftClient<SocialGraphServiceClient>> social_graph_client_pool(
      "social-graph-client", social_graph_addr, social_graph_port, 0,
      social_graph_conns, social_graph_timeout, social_graph_keepalive,
      config_json);

  Redis redis_client_pool =
      init_redis_client_pool(config_json, "home-timeline");
  std::shared_ptr<TServerSocket> server_socket =
      get_server_socket(config_json, "0.0.0.0", port);

  TThreadedServer server(std::make_shared<HomeTimelineServiceProcessor>(
                             std::make_shared<HomeTimelineHandler>(
                                 &redis_client_pool, &compose_post_client_pool,
                                 &social_graph_client_pool)),
                         server_socket,
                         std::make_shared<TFramedTransportFactory>(),
                         std::make_shared<TBinaryProtocolFactory>());

  LOG(info) << "Starting the home-timeline-service server...";
  server.serve();
}

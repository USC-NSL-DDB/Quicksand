#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <nlohmann/json.hpp>

#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../logger.h"
#include "../utils.h"
#include "../utils_mongodb.h"
#include "../utils_redis.h"
#include "../utils_thrift.h"
#include "UserTimelineHandler.h"

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

  int port = config_json["user-timeline-service"]["port"];

  int compose_post_port = config_json["compose-post-service"]["port"];
  std::string compose_post_addr = config_json["compose-post-service"]["addr"];
  int compose_post_conns = config_json["compose-post-service"]["connections"];
  int compose_post_timeout = config_json["compose-post-service"]["timeout_ms"];
  int compose_post_keepalive =
      config_json["compose-post-service"]["keepalive_ms"];

  int mongodb_conns = config_json["user-timeline-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-timeline-mongodb"]["timeout_ms"];

  auto mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user-timeline", mongodb_conns);

  if (mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  ClientPool<ThriftClient<ComposePostServiceClient>> compose_post_client_pool(
      "compose-post-client", compose_post_addr, compose_post_port, 0,
      compose_post_conns, compose_post_timeout, compose_post_keepalive, config_json);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "user-timeline", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  Redis redis_client_pool =
      init_redis_client_pool(config_json, "user-timeline");
  std::shared_ptr<TServerSocket> server_socket = get_server_socket(config_json, "0.0.0.0", port);

  TThreadedServer server(std::make_shared<UserTimelineServiceProcessor>(
                             std::make_shared<UserTimelineHandler>(
                                 &redis_client_pool, mongodb_client_pool,
                                 &compose_post_client_pool)),
                         server_socket,
                         std::make_shared<TFramedTransportFactory>(),
                         std::make_shared<TBinaryProtocolFactory>());

  LOG(info) << "Starting the user-timeline-service server...";
  server.serve();
}

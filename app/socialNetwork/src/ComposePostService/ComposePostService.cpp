#include <nu/runtime.hpp>
#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include "../utils.h"
#include "../utils_thrift.h"
#include "ComposePostHandler.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

nu::Runtime::Mode mode;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

void do_work() {
  signal(SIGINT, sigintHandler);

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["compose-post-service"]["port"];

  int user_port = config_json["user-service"]["port"];
  std::string user_addr = config_json["user-service"]["addr"];
  int user_conns = config_json["user-service"]["connections"];
  int user_timeout = config_json["user-service"]["timeout_ms"];
  int user_keepalive = config_json["user-service"]["keepalive_ms"];

  int home_timeline_port = config_json["home-timeline-service"]["port"];
  std::string home_timeline_addr = config_json["home-timeline-service"]["addr"];
  int home_timeline_conns = config_json["home-timeline-service"]["connections"];
  int home_timeline_timeout =
      config_json["home-timeline-service"]["timeout_ms"];
  int home_timeline_keepalive =
      config_json["home-timeline-service"]["keepalive_ms"];

  ClientPool<ThriftClient<UserServiceClient>> user_client_pool(
      "user-service-client", user_addr, user_port, 0, user_conns, user_timeout,
      user_keepalive, config_json);
  ClientPool<ThriftClient<HomeTimelineServiceClient>> home_timeline_client_pool(
      "home-timeline-service-client", home_timeline_addr, home_timeline_port, 0,
      home_timeline_conns, home_timeline_timeout, home_timeline_keepalive,
      config_json);

  std::shared_ptr<TServerSocket> server_socket =
      get_server_socket(config_json, "0.0.0.0", port);

  auto compose_post_handler = std::make_shared<ComposePostHandler>(
      &user_client_pool, &home_timeline_client_pool);

  rt::Thread([compose_post_handler = compose_post_handler.get()] {
    compose_post_handler->poller();
  }).Detach();

  TThreadedServer server(std::make_shared<ComposePostServiceProcessor>(
                             std::move(compose_post_handler)),
                         server_socket,
                         std::make_shared<TFramedTransportFactory>(),
                         std::make_shared<TBinaryProtocolFactory>());
  LOG(info) << "Starting the compose-post-service server ...";
  server.serve();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

#include <iostream>
#include <nu/runtime.hpp>

#include "ThriftBackEndServer.hpp"
#include "utils.hpp"

using namespace social_network;

void DoWork() {
  json config_json;

  BUG_ON(LoadConfigFile("config/service-config.json", &config_json) != 0);

  auto port = config_json["back-end-service"]["port"];
  std::cout << "port = " << port << std::endl;
  std::shared_ptr<TServerSocket> server_socket =
      std::make_shared<TServerSocket>("0.0.0.0", port);

  auto secret = config_json["secret"];
  auto back_end_handler =
      std::make_shared<ThriftBackEndServer>(secret);

  TThreadedServer server(
      std::make_shared<BackEndServiceProcessor>(std::move(back_end_handler)),
      server_socket, std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>());
  std::cout << "Starting the ThriftBackEndServer..." << std::endl;
  server.serve();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { DoWork(); });
}

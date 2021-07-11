#include <iostream>
#include <nu/runtime.hpp>

#include "ThriftBackEndServer.hpp"
#include "utils.hpp"

using namespace social_network;

void do_work() {
  json config_json;

  BUG_ON(load_config_file("config/service-config.json", &config_json) != 0);

  auto port = config_json["back-end-service"]["port"];
  std::cout << "port = " << port << std::endl;
  std::shared_ptr<TServerSocket> server_socket =
      std::make_shared<TServerSocket>("0.0.0.0", port);

  auto secret = config_json["secret"];
  std::string netif = config_json["back-end-service"]["netif"];
  auto machine_id = GetMachineId(netif);
  BUG_ON(machine_id == "");
  std::cout << "machine_id = " << machine_id << std::endl;
  auto back_end_handler =
      std::make_shared<ThriftBackEndServer>(machine_id, secret);

  TThreadedServer server(
      std::make_shared<BackEndServiceProcessor>(std::move(back_end_handler)),
      server_socket, std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>());
  std::cout << "Starting the ThriftBackEndServer..." << std::endl;
  server.serve();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

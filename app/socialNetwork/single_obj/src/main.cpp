#include <iostream>
#include <nu/monitor.hpp>
#include <nu/rem_obj.hpp>
#include <nu/runtime.hpp>

#include "ThriftBackEndServer.hpp"
#include "utils.hpp"

using namespace social_network;

constexpr bool kEnableMigration = false;

class ServiceEntry {
public:
  ServiceEntry() {
    json config_json;

    BUG_ON(LoadConfigFile("config/service-config.json", &config_json) != 0);

    auto port = config_json["back-end-service"]["port"];
    std::cout << "port = " << port << std::endl;
    std::shared_ptr<TServerSocket> server_socket =
        std::make_shared<TServerSocket>("0.0.0.0", port);

    auto secret = config_json["secret"];
    auto back_end_handler = std::make_shared<ThriftBackEndServer>(secret);

    TThreadedServer server(
        std::make_shared<BackEndServiceProcessor>(std::move(back_end_handler)),
        server_socket, std::make_shared<TFramedTransportFactory>(),
        std::make_shared<TBinaryProtocolFactory>());
    std::cout << "Starting the ThriftBackEndServer..." << std::endl;
    server.serve();
  }
};

namespace nu {
class Test {
public:
  Test(uint32_t pressure_mem_mbs) : pressure_mem_mbs_(pressure_mem_mbs) {}

  int migrate() {
    Resource resource = {.cores = 0, .mem_mbs = pressure_mem_mbs_};
    Runtime::monitor->mock_set_pressure(resource);
    return 0;
  }

private:
  uint32_t pressure_mem_mbs_;
};
} // namespace nu

void DoWork() {
  netaddr addr;
  addr.ip = MAKE_IP_ADDR(18, 18, 1, 2);
  addr.port = nu::ObjServer::kObjServerPort;
  auto thrift_future = nu::async([&] {
    auto thrift_back_end_server =
        nu::RemObj<ServiceEntry>::create_pinned_at(addr);
  });
  if constexpr (kEnableMigration) {
    auto test = nu::RemObj<nu::Test>::create_pinned(32 * 1024);
    std::cout << "Press enter to start migration..." << std::endl;
    std::cin.ignore();
    std::cout << "Start migrating..." << std::endl;
    test.run(&nu::Test::migrate);
  }
  rt::Yield();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { DoWork(); });
}

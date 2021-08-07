#include <iostream>
#include <memory>
#include <nu/monitor.hpp>
#include <nu/rem_obj.hpp>
#include <nu/runtime.hpp>

#include "ThriftBackEndServer.hpp"

constexpr uint32_t kNumEntryObjs = 2;
constexpr bool kEnableMigration = false;

using namespace social_network;

class ServiceEntry {
public:
  ServiceEntry(StateCaps caps) {
    json config_json;

    BUG_ON(LoadConfigFile("config/service-config.json", &config_json) != 0);

    auto port = config_json["back-end-service"]["port"];
    std::cout << "port = " << port << std::endl;
    std::shared_ptr<TServerSocket> server_socket =
        std::make_shared<TServerSocket>("0.0.0.0", port);

    auto secret = config_json["secret"];
    auto back_end_handler = std::make_shared<ThriftBackEndServer>(caps);

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
  auto states = std::make_unique<States>();

  std::vector<nu::Future<void>> thrift_futures;
  for (uint32_t i = 0; i < kNumEntryObjs; i++) {
    thrift_futures.emplace_back(nu::async(
        [&] { nu::RemObj<ServiceEntry>::create_pinned(states->get_caps()); }));
  }

  if constexpr (kEnableMigration) {
    auto test = nu::RemObj<nu::Test>::create_pinned(32 * 1024);
    std::cout << "Press enter to start migration..." << std::endl;
    std::cin.ignore();
    std::cout << "Start migrating..." << std::endl;
    test.run(&nu::Test::migrate);
  }
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { DoWork(); });
}

#include <iostream>
#include <memory>
#include <nu/rem_obj.hpp>
#include <nu/runtime.hpp>

#include "ThriftBackEndServer.hpp"

using namespace social_network;

const static uint32_t kEntryObjIps[] = {
    MAKE_IP_ADDR(18, 18, 1, 2),  MAKE_IP_ADDR(18, 18, 1, 5),
    MAKE_IP_ADDR(18, 18, 1, 7),  MAKE_IP_ADDR(18, 18, 1, 8),
    MAKE_IP_ADDR(18, 18, 1, 9),  MAKE_IP_ADDR(18, 18, 1, 10),
    MAKE_IP_ADDR(18, 18, 1, 11), MAKE_IP_ADDR(18, 18, 1, 12),
    MAKE_IP_ADDR(18, 18, 1, 13), MAKE_IP_ADDR(18, 18, 1, 14),
    MAKE_IP_ADDR(18, 18, 1, 15), MAKE_IP_ADDR(18, 18, 1, 16),
    MAKE_IP_ADDR(18, 18, 1, 17), MAKE_IP_ADDR(18, 18, 1, 18),
    MAKE_IP_ADDR(18, 18, 1, 19), MAKE_IP_ADDR(18, 18, 1, 20),
    MAKE_IP_ADDR(18, 18, 1, 21), MAKE_IP_ADDR(18, 18, 1, 22),
    MAKE_IP_ADDR(18, 18, 1, 23), MAKE_IP_ADDR(18, 18, 1, 24),
    MAKE_IP_ADDR(18, 18, 1, 25), MAKE_IP_ADDR(18, 18, 1, 26),
    MAKE_IP_ADDR(18, 18, 1, 27), MAKE_IP_ADDR(18, 18, 1, 28),
    MAKE_IP_ADDR(18, 18, 1, 29), MAKE_IP_ADDR(18, 18, 1, 30),
    MAKE_IP_ADDR(18, 18, 1, 31), MAKE_IP_ADDR(18, 18, 1, 32),
    MAKE_IP_ADDR(18, 18, 1, 33), MAKE_IP_ADDR(18, 18, 1, 34),    
};

class ServiceEntry {
public:
  ServiceEntry(StateCaps caps) {
    json config_json;

    BUG_ON(LoadConfigFile("config/service-config.json", &config_json) != 0);

    auto port = config_json["back-end-service"]["port"];
    std::cout << "port = " << port << std::endl;
    std::shared_ptr<TServerSocket> server_socket =
        std::make_shared<TServerSocket>("0.0.0.0", port);

    caps.secret = config_json["secret"];
    auto back_end_handler = std::make_shared<ThriftBackEndServer>(caps);

    server_ = std::make_unique<TThreadedServer>(
        std::make_shared<BackEndServiceProcessor>(std::move(back_end_handler)),
        server_socket, std::make_shared<TFramedTransportFactory>(),
        std::make_shared<TBinaryProtocolFactory>());
  }

  void start() {
    std::cout << "Starting the ThriftBackEndServer..." << std::endl;
    server_->serve();
  }

private:
  std::unique_ptr<TThreadedServer> server_;
};

void do_work() {
  auto states = std::make_unique<States>();

  std::vector<nu::RemObj<ServiceEntry>> service_entry_objs;
  std::vector<nu::Future<void>> futures;
  for (auto entry_obj_ip : kEntryObjIps) {
    service_entry_objs.emplace_back(nu::RemObj<ServiceEntry>::create_pinned_at(
        entry_obj_ip, states->get_caps()));
    futures.emplace_back(
        service_entry_objs.back().run_async(&ServiceEntry::start));
  }
  std::cout << "Done creating proclets..." << std::endl;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

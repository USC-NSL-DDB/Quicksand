#include <iostream>
#include <memory>
#include <nu/rem_obj.hpp>
#include <nu/runtime.hpp>

#include "ThriftBackEndServer.hpp"

using namespace social_network;

const static uint32_t kEntryObjIps[] = {MAKE_IP_ADDR(18, 18, 1, 2),
                                        MAKE_IP_ADDR(18, 18, 1, 5)};

bool signalled = false;

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

void wait_for_signal() {
  while (!rt::access_once(signalled)) {
    timer_sleep(100);
  }
  rt::access_once(signalled) = false;
}

void do_work() {
  auto states = std::make_unique<States>();

  std::cout << "creating proxy 0..." << std::endl;
  auto service_entry_0 = nu::RemObj<ServiceEntry>::create_pinned_at(
      kEntryObjIps[0], states->get_caps());

  std::cout << "waiting for signal..." << std::endl;
  wait_for_signal();

  std::cout << "creating proxy 1..." << std::endl;
  auto service_entry_1 = nu::RemObj<ServiceEntry>::create_pinned_at(
      kEntryObjIps[1], states->get_caps());

  auto future_0 = service_entry_0.run_async(&ServiceEntry::start);
  auto future_1 = service_entry_1.run_async(&ServiceEntry::start);
}

void signal_handler(int signum) { rt::access_once(signalled) = true; }

int main(int argc, char **argv) {
  signal(SIGHUP, signal_handler);
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

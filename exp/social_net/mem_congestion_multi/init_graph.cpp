#include <fstream>
#include <memory>
#include <nu/commons.hpp>
#include <nu/utils/future.hpp>
#include <runtime.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "../gen-cpp/BackEndService.h"
#include "../gen-cpp/social_network_types.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

const std::string kFilePath =
    "datasets/social-graph/socfb-Princeton12/socfb-Princeton12.mtx";
const std::string kProxyIp = "18.18.1.2";
constexpr static uint32_t kEntryObjPort = 9091;

class ClientPtr {
public:
  ClientPtr(const std::string &ip) {
    socket.reset(new TSocket(ip, kEntryObjPort));
    transport.reset(new TFramedTransport(socket));
    protocol.reset(new TBinaryProtocol(transport));
    client.reset(new social_network::BackEndServiceClient(protocol));
    transport->open();
  }

  social_network::BackEndServiceClient *operator->() { return client.get(); }

private:
  std::shared_ptr<TTransport> socket;
  std::shared_ptr<TTransport> transport;
  std::shared_ptr<TProtocol> protocol;
  std::unique_ptr<social_network::BackEndServiceClient> client;
};

void do_work() {
  std::ifstream ifs(kFilePath);

  uint32_t num_nodes, num_edges;
  ifs >> num_nodes >> num_nodes >> num_edges;
  std::cout << "num_nodes = " << num_nodes << std::endl;
  std::cout << "num_edges = " << num_edges << std::endl;

  ClientPtr client(kProxyIp);
  for (uint32_t i = 0; i < num_nodes; i++) {
    int64_t user_id = i + 1;
    std::string first_name = "first_name_" + std::to_string(user_id);
    std::string last_name = "last_name_" + std::to_string(user_id);
    std::string username = "username_" + std::to_string(user_id);
    std::string password = "password_" + std::to_string(user_id);
    client->RegisterUserWithId(first_name, last_name, username, password,
                               user_id);
  }

  for (uint32_t i = 0; i < num_edges; i++) {
    int64_t user_id_x, user_id_y;
    ifs >> user_id_x >> user_id_y;
    client->Follow(user_id_x, user_id_y);
    client->Follow(user_id_y, user_id_x);
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

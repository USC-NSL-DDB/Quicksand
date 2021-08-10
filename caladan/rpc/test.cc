extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <unistd.h>
}

#include <chrono>
#include <iostream>
#include <memory>

#include "runtime.h"
#include "rpc.h"

namespace {

using namespace std::chrono;
using sec = duration<double>;

nu::RPCReturnBuffer ServerHandler(std::span<const std::byte> args) {
  auto buf = std::make_unique<std::byte[]>(args.size());
  std::copy(args.begin(), args.end(), buf.get());
  return nu::RPCReturnBuffer({buf.get(), args.size()}, [b = std::move(buf)] {});
}

void RunServer() {
  nu::RPCServerInit(&ServerHandler);
}

void RunClient(netaddr raddr, int threads, int samples, size_t buflen) {
  std::unique_ptr<nu::RPCClient> c = nu::RPCClient::Dial(raddr);
  std::vector<rt::Thread> workers;

  // |--- start experiment duration timing ---|
  barrier();
  auto start = steady_clock::now();
  barrier();    

  for (int i = 0; i < threads; ++i) {
    workers.emplace_back([c = c.get(), samples, buflen] {
      auto buf = std::make_unique<std::byte[]>(buflen);
      for (int i = 0; i < samples; ++i) c->Call({buf.get(), buflen});
    });
  }
  for (auto &t : workers) t.Join();

  // |--- end experiment duration timing ---|
  barrier();
  auto finish = steady_clock::now();
  barrier();

  // report results
  double seconds = duration_cast<sec>(finish - start).count();
  size_t reqs = samples * threads;
  size_t mbytes = buflen * reqs / 1000 / 1000;
  double mbytes_per_second = static_cast<double>(mbytes) / seconds;
  double reqs_per_second = static_cast<double>(reqs) / seconds;
  std::cout << "transferred " << mbytes_per_second << " MB/s" << std::endl;
  std::cout << "transferred " << reqs_per_second << " reqs/s" << std::endl;
}

int StringToAddr(const char *str, uint32_t *addr) {
  uint8_t a, b, c, d;
  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) return -EINVAL;
  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

}  // anonymous

int main(int argc, char *argv[]) {
  if (argc < 3) {
    std::cerr << "usage: [cfg_file] [command] ..." << std::endl;
    std::cerr << "commands>" << std::endl;
    std::cerr << "\tserver - runs an RPC server" << std::endl;
    std::cerr << "\tclinet - runs an RPC client" << std::endl;
    return -EINVAL;
  }

  std::string cmd = argv[2];
  netaddr raddr = {};
  int threads = 0, samples = 0;
  size_t buflen = 0;

  if (cmd.compare("client") == 0) {
    if (argc != 7) {
      std::cerr << "usage: [cfg_file] " << cmd << " [ip_addr] [threads] "
                << "[samples] [buflen]" << std::endl;
      return -EINVAL;
    }

    int ret = StringToAddr(argv[3], &raddr.ip);
    if (ret) return -EINVAL;
    threads = std::stoi(argv[4], nullptr, 0);
    samples = std::stoi(argv[5], nullptr, 0);
    buflen = std::stoul(argv[6], nullptr, 0);
  } else if (cmd.compare("server") != 0) {
    std::cerr << "invalid command: " << cmd << std::endl;
    return -EINVAL;
  }

  return rt::RuntimeInit(argv[1], [=] {
    std::string cmd = argv[2];
    if (cmd.compare("server") == 0) {
      RunServer();
    } else if (cmd.compare("client") == 0) {
      RunClient(raddr, threads, samples, buflen);
    }
  });
}

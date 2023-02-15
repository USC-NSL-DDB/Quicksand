#include <net.h>
#include <runtime.h>

#include <iostream>

constexpr auto kServerIP = MAKE_IP_ADDR(18, 18, 1, 2);
constexpr auto kClientIP = MAKE_IP_ADDR(18, 18, 1, 4);
constexpr auto kPort = 6900;
constexpr auto kMeasureTimes = 1 << 20;

bool dummy;

void do_server() {
  netaddr laddr{.ip = 0, .port = kPort};
  auto *tcp_queue = rt::TcpQueue::Listen(laddr, 1);
  BUG_ON(!tcp_queue);
  auto *tcp_conn = tcp_queue->Accept();
  BUG_ON(!tcp_conn);
  barrier();
  std::cout << "Start tsc = " << rdtsc() << std::endl;
  barrier();  

  double sum = 0;
  for (uint32_t i = 0; i < kMeasureTimes; i++) {
    barrier();
    auto start_local_tsc = rdtsc();
    barrier();
    BUG_ON(tcp_conn->WriteFull(&dummy, sizeof(dummy), /* nt = */ false,
                               /* poll = */ true) != sizeof(dummy));
    uint64_t remote_tsc;
    BUG_ON(tcp_conn->ReadFull(&remote_tsc, sizeof(remote_tsc),
                              /* nt = */ false,
                              /* poll = */ true) != sizeof(remote_tsc));
    barrier();
    auto end_local_tsc = rdtsc();
    barrier();
    auto mid_local_tsc = (start_local_tsc + end_local_tsc) / 2;
    sum += mid_local_tsc - remote_tsc;
  }
  std::cout << "Server's tsc is " << static_cast<uint64_t>(sum / kMeasureTimes)
            << " cycles faster than the client." << std::endl;
}

void do_client() {
  netaddr laddr{.ip = 0, .port = 0};
  netaddr raddr{.ip = kServerIP, .port = kPort};
  auto *tcp_conn = rt::TcpConn::Dial(laddr, raddr);
  BUG_ON(!tcp_conn);
  barrier();
  std::cout << "Start tsc = " << rdtsc() << std::endl;
  barrier();
  for (uint32_t i = 0; i < kMeasureTimes; i++) {
    BUG_ON(tcp_conn->ReadFull(&dummy, sizeof(dummy), /* nt = */ false,
                              /* poll = */ true) != sizeof(dummy));
    barrier();
    auto tsc = rdtsc();
    barrier();
    BUG_ON(tcp_conn->WriteFull(&tsc, sizeof(tsc), /* nt = */ false,
                               /* poll = */ true) != sizeof(tsc));
  }
}

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
    if (get_cfg_ip() == kServerIP) {
      do_server();
    } else if (get_cfg_ip() == kClientIP) {
      do_client();
    } else {
      BUG();
    }
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

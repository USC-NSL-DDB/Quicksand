#include <atomic>
#include <iostream>

#include "net.h"
#include "runtime.h"
#include "sync.h"
#include "thread.h"
#include "timer.h"

constexpr uint32_t kPort = 8010;
constexpr uint32_t kNumNodes = 2;
constexpr uint32_t kSlotUs = 10 * rt::kMilliseconds;

std::atomic<uint32_t> num_active_ths;
std::atomic<uint64_t> yield_ddl;
std::vector<rt::TcpConn *> accepted_conns;
std::vector<rt::TcpConn *> dialed_conns;

void listen() {
  netaddr laddr = {.ip = 0, .port = kPort};
  auto *queue = rt::TcpQueue::Listen(laddr, 128);
  while (accepted_conns.size() != rt::RuntimeMaxCores()) {
    accepted_conns.push_back(queue->Accept());
  }
}

void worker_fn(bool first, rt::TcpConn *accepted_conn,
               rt::TcpConn *dialed_conn) {
  bool dummy;

  while (true) {
    if (!first) {
      dialed_conn->ReadFull(&dummy, sizeof(dummy));
    }
    first = false;

    if ((num_active_ths++) % rt::RuntimeMaxCores() == 0) {
      yield_ddl = rt::MicroTime() + kSlotUs;
    }
    while (num_active_ths % rt::RuntimeMaxCores() ||
           rt::MicroTime() < yield_ddl)
      ;

    accepted_conn->WriteFull(&dummy, sizeof(dummy), false, true);
  }
}

void do_work() {
  rt::Thread listen_th([] { listen(); });
  rt::Sleep(10 * rt::kSeconds);

  bool first;
  netaddr raddr;
  raddr.port = kPort;
  if ((get_cfg_ip() & 255) != kNumNodes) {
    raddr.ip = get_cfg_ip() + 1;
    first = false;
  } else {
    raddr.ip = (get_cfg_ip() & (~255)) | 1;
    first = true;
  }

  for (uint32_t i = 0; i < rt::RuntimeMaxCores(); i++) {
    dialed_conns.push_back(rt::TcpConn::DialAffinity(i, raddr));
  }
  listen_th.Join();

  std::vector<rt::Thread> workers;
  for (uint32_t i = 0; i < rt::RuntimeMaxCores(); i++) {
    workers.emplace_back([first, accepted_conn = accepted_conns[i],
                          dialed_conn = dialed_conns[i]] {
      worker_fn(first, accepted_conn, dialed_conn);
    });
  }

  rt::Preempt p;
  rt::PreemptGuardAndPark gp(&p);
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

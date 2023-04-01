#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sharded_service.hpp"
#include "nu/utils/perf.hpp"
#include "nu/utils/thread.hpp"

constexpr auto kClientIP = MAKE_IP_ADDR(18, 18, 1, 100);
constexpr auto kDelayUs = 50;
constexpr auto kNumThreads = 100;
constexpr auto kTargetMops = 1;
constexpr auto kNumSeconds = 1;

struct Obj {
  using Key = uint64_t;

  void work() {
    // preempt_disable();
    delay_us(kDelayUs);
    // preempt_enable();
  }

  bool empty() const { return false; }
  std::size_t size() const { return 1; }
  void split(Key *mid_k, Obj *latter_half) {}
};

struct BenchPerfThreadState : public nu::PerfThreadState {
  BenchPerfThreadState(nu::ShardedStatelessService<Obj> s)
      : service(std::move(s)) {}

  nu::ShardedStatelessService<Obj> service;
};

struct BenchPerfAdapter : public nu::PerfAdapter {
  BenchPerfAdapter(nu::ShardedStatelessService<Obj> s)
      : service(std::move(s)) {}

  std::unique_ptr<nu::PerfThreadState> create_thread_state() override {
    auto *state = new BenchPerfThreadState(service);
    return std::unique_ptr<nu::PerfThreadState>(state);
  }

  std::unique_ptr<nu::PerfRequest> gen_req(
      nu::PerfThreadState *state) override {
    return std::make_unique<nu::PerfRequest>();
  }

  bool serve_req(nu::PerfThreadState *raw_state,
                 const nu::PerfRequest *req) override {
    auto *state = dynamic_cast<BenchPerfThreadState *>(raw_state);
    state->service.run(&Obj::work);
    return true;
  }

  nu::ShardedStatelessService<Obj> service;
};

struct Client {
  Client() = default;
  void run(nu::ShardedStatelessService<Obj> service) {
    BenchPerfAdapter adapter(std::move(service));
    nu::Perf perf(adapter);
    perf.run(kNumThreads, kTargetMops,
             /* duration_us = */ kNumSeconds * nu::kOneSecond,
             /* warmup_us */ nu::kOneSecond);
    std::cout << "real_mops, avg_lat, 50th_lat, 90th_lat, 95th_lat, 99th_lat, "
                 "99.9th_lat"
              << std::endl;
    std::cout << perf.get_real_mops() << " " << perf.get_average_lat() << " "
              << perf.get_nth_lat(50) << " " << perf.get_nth_lat(90) << " "
              << perf.get_nth_lat(95) << " " << perf.get_nth_lat(99) << " "
              << perf.get_nth_lat(99.9) << std::endl;
  }

  nu::ShardedStatelessService<Obj> service;
};

void bench() {
  auto service = nu::make_sharded_stateless_service<Obj>();
  auto client = nu::make_proclet<Client>(true, std::nullopt, kClientIP);
  client.run(&Client::run, service);
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { bench(); });
}

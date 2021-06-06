#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <signal.h>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "cereal/types/optional.hpp"
#include "cereal/types/string.hpp"
#include "dis_hash_table.hpp"
#include "monitor.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"
#include "utils/farmhash.hpp"
#include "utils/trace_logger.hpp"

using namespace nu;

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr double kLoadFactor = 0.20;
constexpr uint32_t kNumThreads = 100;
constexpr uint32_t kNumRecordsTotal = 400 << 20;
constexpr uint32_t kNumRecordsPerCore = kNumRecordsTotal / kNumCores;
constexpr double kTargetMOPS = 3;
constexpr bool kEnablePrinting = true;
constexpr uint32_t kPrintIntervalUS = 200 * 1000;
constexpr uint32_t kMigrationTriggeredUs = 5 * kPrintIntervalUS;

Runtime::Mode mode;
std::unique_ptr<TraceLogger> trace_logger;
bool done = false;

struct Key {
  char data[kKeyLen];

  bool operator==(const Key &o) const {
    return __builtin_memcmp(data, o.data, kKeyLen) == 0;
  }

  template <class Archive> void serialize(Archive &ar) { ar(data); }
};

struct Val {
  char data[kValLen];

  template <class Archive> void serialize(Archive &ar) { ar(data); }
};

namespace nu {

class Test {
public:
  constexpr static auto kFarmHashKeytoU64 = [](const Key &key) {
    return util::Hash64(key.data, kKeyLen);
  };
  using DSHashTable =
      DistributedHashTable<Key, Val, decltype(kFarmHashKeytoU64)>;
  constexpr static size_t kNumPairs =
      DSHashTable::kNumShards * DSHashTable::kNumBucketsPerShard * kLoadFactor;

  Test(uint32_t pressure_mem_mbs) : pressure_mem_mbs_(pressure_mem_mbs) {}

  int migrate() {
    Resource resource = {.cores = 0, .mem_mbs = pressure_mem_mbs_};
    Runtime::monitor->mock_set_pressure(resource);
    return 0;
  }

  void print_addrs() {
    std::cout << reinterpret_cast<uintptr_t>(
                     ObjServer::construct_obj<DSHashTable::HashTableShard>)
              << std::endl;
    std::cout << reinterpret_cast<uintptr_t>(
                     ObjServer::construct_obj<Test, int>)
              << std::endl;
    auto md_ptr_0 = &DSHashTable::HashTableShard::template get_with_hash<Key>;
    std::cout << *reinterpret_cast<unsigned long *>(&md_ptr_0) << std::endl;
    auto md_ptr_1 =
        &DSHashTable::HashTableShard::template put_with_hash<Key, Val>;
    std::cout << *reinterpret_cast<unsigned long *>(&md_ptr_1) << std::endl;
    auto md_ptr_2 = &Test::migrate;
    std::cout << *reinterpret_cast<unsigned long *>(&md_ptr_2) << std::endl;
  }

private:
  uint32_t pressure_mem_mbs_;
};
} // namespace nu

void client_cleanup() {
  ACCESS_ONCE(done) = true;
  trace_logger->disable_print();
}

void sigint_handler(int sig) {
  if (mode != Runtime::Mode::CLIENT) {
    std::cout << "Force exiting..." << std::endl;
    exit(0);
  } else {
    client_cleanup();
  }
}

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = dist(mt);
  }
}

void init(Test::DSHashTable *hash_table, std::vector<Key> *keys) {
  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist('A', 'z');
      auto num_pairs = Test::kNumPairs / kNumThreads;
      for (size_t j = 0; j < num_pairs; j++) {
        Key key;
        Val val;
        random_str(dist, mt, kKeyLen, key.data);
        random_str(dist, mt, kValLen, val.data);
        keys[tid].push_back(key);
        hash_table->put(key, val);
      }
    });
  }
  for (auto &thread : threads) {
    thread.Join();
  }
  trace_logger.reset(new TraceLogger());
}

void benchmark(Test::DSHashTable *hash_table, std::vector<Key> *keys,
               RemObj<nu::Test> *test) {
  std::vector<std::pair<uint64_t, uint64_t>> records[kNumCores];
  for (uint32_t i = 0; i < kNumCores; i++) {
    records[i].reserve(kNumRecordsPerCore);
  }

  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      auto target_mops = kTargetMOPS / kNumThreads;
      uint64_t target_cycles_per_op = cycles_per_us / target_mops;
      uint64_t target_next_op_tsc = rdtsc();

      while (true) {
        for (const auto &k : keys[tid]) {
          if (unlikely(ACCESS_ONCE(done))) {
            return;
          }

          auto tsc = rdtsc();
          if (tsc < target_next_op_tsc) {
            timer_sleep((target_next_op_tsc - tsc) / cycles_per_us);
          }
          target_next_op_tsc += target_cycles_per_op;

          uint64_t start_tsc, end_tsc;
          if constexpr (kEnablePrinting) {
            auto p = trace_logger->add_trace([&] { hash_table->get(k); });
            start_tsc = p.first;
            end_tsc = p.second;
          } else {
            start_tsc = rdtsc();
            hash_table->get(k);
            end_tsc = rdtsc();
          }

          int cpu_id = get_cpu();
          if (unlikely(records[cpu_id].size() == kNumRecordsPerCore)) {
            client_cleanup();
            put_cpu();
            return;
          }
          records[cpu_id].push_back(
              std::make_pair(start_tsc, end_tsc - start_tsc));
          put_cpu();
        }
      }
    });
  }

  if constexpr (kEnablePrinting) {
    trace_logger->enable_print(kPrintIntervalUS);
  }
  timer_sleep(kMigrationTriggeredUs);
  test->run(&Test::migrate);

  for (auto &thread : threads) {
    thread.Join();
  }

  std::vector<std::pair<uint64_t, uint64_t>> all_records;
  for (uint32_t i = 0; i < kNumCores; i++) {
    all_records.insert(all_records.end(), records[i].begin(), records[i].end());
  }
  sort(all_records.begin(), all_records.end());
  std::ofstream ofs("records");
  for (auto [start, duration] : all_records) {
    ofs << start << " " << duration << std::endl;
  }
}

void do_work() {
  Test::DSHashTable hash_table;
  std::vector<Key> keys[kNumThreads];

  std::cout << "start initing..." << std::endl;
  auto test = RemObj<nu::Test>::create_pinned(32 * 1024);
  init(&hash_table, keys);
  std::cout << "start benchmarking..." << std::endl;
  benchmark(&hash_table, keys, &test);
}

int main(int argc, char **argv) {
  signal(SIGINT, sigint_handler);

  int ret;
  std::string mode_str;

  if (argc < 3) {
    goto wrong_args;
  }

  mode_str = std::string(argv[2]);
  if (mode_str == "CLT") {
    mode = Runtime::Mode::CLIENT;
  } else if (mode_str == "SRV") {
    mode = Runtime::Mode::SERVER;
  } else if (mode_str == "CTL") {
    mode = Runtime::Mode::CONTROLLER;
  } else {
    goto wrong_args;
  }

  ret = rt::RuntimeInit(std::string(argv[1]), [] {
    std::cout << "Running " << __FILE__ "..." << std::endl;
    netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
    auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                                 /* local_migrator_port = */ 8002,
                                 /* remote_ctrl_addr = */ remote_ctrl_addr,
                                 /* mode = */ mode);
    do_work();
  });

  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL" << std::endl;
  return -EINVAL;
}

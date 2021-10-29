#include <algorithm>
#include <cereal/types/optional.hpp>
#include <cereal/types/string.hpp>
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

#include "nu/dis_hash_table.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"
#include "nu/utils/trace_logger.hpp"

using namespace nu;

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr double kLoadFactor = 0.20;
constexpr uint32_t kNumThreads = 100;
constexpr uint32_t kNumRecordsTotal = 100 << 20;
constexpr uint32_t kNumRecordsPerCore = kNumRecordsTotal / kNumCores;
constexpr double kTargetMOPS = 3;
constexpr bool kEnablePrinting = true;
constexpr uint32_t kPrintIntervalUS = 200 * 1000;
constexpr uint32_t kMigrationTriggeredUs = 10 * kPrintIntervalUS;

constexpr auto kIPServer0 = MAKE_IP_ADDR(18, 18, 1, 2); // The migration source.
constexpr auto kIPServer1 = MAKE_IP_ADDR(18, 18, 1, 5); // The migration dest.

Runtime::Mode mode;
std::unique_ptr<TraceLogger> trace_loggers[2];
bool done = false;

struct Key {
  char data[kKeyLen];

  bool operator==(const Key &o) const {
    return __builtin_memcmp(data, o.data, kKeyLen) == 0;
  }

  template <class Archive> void serialize(Archive &ar) {
    ar(cereal::binary_data(data, sizeof(data)));
  }
};

struct Val {
  char data[kValLen];

  template <class Archive> void serialize(Archive &ar) {
    ar(cereal::binary_data(data, sizeof(data)));
  }
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
      (1 << DSHashTable::kDefaultPowerNumShards) *
      DSHashTable::kNumBucketsPerShard * kLoadFactor;

  Test(uint32_t pressure_mem_mbs) : pressure_mem_mbs_(pressure_mem_mbs) {}

  int migrate() {
    ResourcePressureInfo pressure = {.mem_mbs_to_release = pressure_mem_mbs_,
                                     .num_cores_to_release = 0};
    PressureHandler::mock_set_pressure(pressure);
    return 0;
  }

private:
  uint32_t pressure_mem_mbs_;
};
} // namespace nu

void client_cleanup() {
  ACCESS_ONCE(done) = true;
  trace_loggers[0]->disable_print();
  trace_loggers[1]->disable_print();
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
  trace_loggers[0].reset(new TraceLogger("************Server0************"));
  trace_loggers[1].reset(new TraceLogger("************Server1************"));
}

void benchmark(Test::DSHashTable *hash_table, std::vector<Key> *keys,
               RemObj<nu::Test> *test) {
  std::vector<std::pair<uint64_t, uint64_t>> records[2][kNumCores];
  for (uint32_t i = 0; i < 2; i++) {
    for (uint32_t j = 0; j < kNumCores; j++) {
      records[i][j].reserve(kNumRecordsPerCore);
    }
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

          auto start_tsc = rdtsc();
          auto [v_optional, ip] = hash_table->get_with_ip(k);
          auto end_tsc = rdtsc();

          uint8_t server_id;
          if (ip == kIPServer0) {
            server_id = 0;
          } else if (ip == kIPServer1) {
            server_id = 1;
          } else {
            BUG();
          }

          if constexpr (kEnablePrinting) {
            auto duration_tsc = end_tsc - start_tsc;
            trace_loggers[server_id]->add_trace(duration_tsc);
          }

          int cpu_id = get_cpu();
          if (unlikely(records[server_id][cpu_id].size() ==
                       kNumRecordsPerCore)) {
            client_cleanup();
            put_cpu();
            return;
          }
          records[server_id][cpu_id].push_back(
              std::make_pair(start_tsc, end_tsc - start_tsc));
          put_cpu();
        }
      }
    });
  }

  if constexpr (kEnablePrinting) {
    trace_loggers[0]->enable_print(kPrintIntervalUS);
    timer_sleep(500 * 1000); // Wait 0.5 seconds to avoid overlapped print.
    trace_loggers[1]->enable_print(kPrintIntervalUS);
  }
  timer_sleep(kMigrationTriggeredUs);
  test->run(&Test::migrate);

  for (auto &thread : threads) {
    thread.Join();
  }

  for (uint8_t server_id = 0; server_id < 2; server_id++) {
    std::vector<std::pair<uint64_t, uint64_t>> all_records;
    for (uint32_t i = 0; i < kNumCores; i++) {
      all_records.insert(all_records.end(), records[server_id][i].begin(),
                         records[server_id][i].end());
    }
    sort(all_records.begin(), all_records.end());
    std::ofstream ofs("records_" + std::to_string(server_id));
    for (auto [start, duration] : all_records) {
      ofs << start << " " << duration << std::endl;
    }
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
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

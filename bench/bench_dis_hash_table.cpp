#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}

#include "cereal/types/optional.hpp"
#include "cereal/types/string.hpp"
#include "dis_hash_table.hpp"
#include "monitor.hpp"
#include "rem_obj.hpp"
#include "runtime.hpp"
#include "utils/farmhash.hpp"

using namespace nu;

Runtime::Mode mode;

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;

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

constexpr auto kFarmHashKeytoU64 = [](const Key &key) {
  return util::Hash64(key.data, kKeyLen);
};

using DSHashTable = DistributedHashTable<Key, Val, decltype(kFarmHashKeytoU64)>;
constexpr double kLoadFactor = 0.25;
constexpr size_t kNumPairs =
    DSHashTable::kNumShards * DSHashTable::kNumBucketsPerShard * kLoadFactor;
constexpr uint32_t kNumThreads = 100;

struct alignas(kCacheLineBytes) AlignedCnt {
  uint64_t cnt;
};

namespace nu {
class Test {
public:
  Test(int x) {}
  int migrate() {
    Resource resource = {.cores = 0, .mem_mbs = 1};
    Runtime::monitor->mock_set_pressure(resource);
    return 0;
  }
};
} // namespace nu

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = dist(mt);
  }
}

void init(DSHashTable *hash_table, std::vector<Key> *keys) {
  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist('A', 'z');
      auto num_pairs = kNumPairs / kNumThreads;
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
}

void benchmark(DSHashTable *hash_table, std::vector<Key> *keys,
               RemObj<nu::Test> *test) {
  AlignedCnt aligned_cnts[kNumThreads];
  memset(aligned_cnts, 0, sizeof(aligned_cnts));

  for (uint32_t i = 0; i < kNumThreads; i++) {
    rt::Thread([&, tid = i] {
      while (true) {
        for (const auto &k : keys[tid]) {
          hash_table->get(k);
          ACCESS_ONCE(aligned_cnts[tid].cnt) = aligned_cnts[tid].cnt + 1;
        }
      }
    }).Detach();
  }

  auto old_us = microtime();
  auto old_sum = 0;
  while (true) {
    test->run(&nu::Test::migrate);
    timer_sleep(1000 * 1000);
    auto cur_us = microtime();
    uint64_t cur_sum = 0;
    for (uint32_t i = 0; i < kNumThreads; i++) {
      cur_sum += ACCESS_ONCE(aligned_cnts[i].cnt);
    }
    auto diff_sum = cur_sum - old_sum;
    auto diff_us = cur_us - old_us;
    auto mops = static_cast<double>(diff_sum) / diff_us;
    preempt_disable();
    std::cout << "mops = " << mops << ", sum = " << diff_sum
              << " , us = " << diff_us << std::endl;
    preempt_enable();
    old_us = cur_us;
    old_sum = cur_sum;
  }
}

void do_work() {
  DSHashTable hash_table;
  std::vector<Key> keys[kNumThreads];

  std::cout << "start initing..." << std::endl;
  auto test = RemObj<nu::Test>::create_pinned(0);
  init(&hash_table, keys);
  std::cout << "start benchmarking..." << std::endl;
  benchmark(&hash_table, keys, &test);
}

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;

  netaddr remote_ctrl_addr = {.ip = MAKE_IP_ADDR(18, 18, 1, 3), .port = 8000};
  auto runtime = Runtime::init(/* local_obj_srv_port = */ 8001,
                               /* local_migrator_port = */ 8002,
                               /* remote_ctrl_addr = */ remote_ctrl_addr,
                               /* mode = */ mode);
  do_work();
}

int main(int argc, char **argv) {
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

  ret = runtime_init(argv[1], _main, NULL);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;

wrong_args:
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL" << std::endl;
  return -EINVAL;
}

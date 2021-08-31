#include <algorithm>
#include <cereal/types/optional.hpp>
#include <cereal/types/string.hpp>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include <runtime.h>

#include "nu/dis_hash_table.hpp"
#include "nu/monitor.hpp"
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"

using namespace nu;

constexpr uint32_t kKeyLen = 16;
constexpr uint32_t kValLen = 2;
constexpr double kLoadFactor = 0.20;
constexpr uint32_t kPrintIntervalUS = 1000 * 1000;
constexpr uint32_t kNumProxies = 2;
constexpr uint32_t kProxyIps[kNumProxies] = {
    MAKE_IP_ADDR(18, 18, 1, 5), MAKE_IP_ADDR(18, 18, 1, 14),
    // MAKE_IP_ADDR(18, 18, 1, 10)
};
constexpr uint32_t kProxyPort = 10086;
constexpr uint32_t kNumThreads = 400;
constexpr uint32_t kBatchSize = 2;

rt::TcpConn *conns[kNumProxies][kNumThreads];

struct alignas(kCacheLineBytes) AlignedCnt {
  uint32_t cnt;
};

AlignedCnt cnts[kNumThreads];

struct Key {
  char data[kKeyLen];
};

struct Val {
  char data[kValLen];
};

struct Req {
  Key key;
  uint32_t proxy_id;
};

struct BatchReq {
  Key keys[kBatchSize];
};

struct BatchResp {
  bool found[kBatchSize];
  Val val[kBatchSize];
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
};
} // namespace nu

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = dist(mt);
  }
}

void gen_reqs(std::vector<Req> *reqs) {
  std::cout << "Generate reqs..." << std::endl;

  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist('A', 'z');
      auto num_pairs = Test::kNumPairs / kNumThreads;
      for (size_t j = 0; j < num_pairs; j++) {
        Key key;
        random_str(dist, mt, kKeyLen, key.data);
        auto shard_id = Test::DSHashTable::get_shard_idx(
            key, Test::DSHashTable::kDefaultPowerNumShards);
        uint8_t proxy_id = shard_id % kNumProxies;
        reqs[tid].push_back({key, proxy_id});
      }
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }
}

void hashtable_get(uint32_t tid, const BatchReq &batch_req, uint32_t proxy_id) {
  BUG_ON(conns[proxy_id][tid]->WriteFull(&batch_req, sizeof(batch_req)) < 0);
  BatchResp batch_resp;
  BUG_ON(conns[proxy_id][tid]->ReadFull(&batch_resp, sizeof(batch_resp)) <= 0);
}

void benchmark(std::vector<Req> *reqs) {
  std::cout << "Start benchmarking..." << std::endl;

  for (uint32_t i = 0; i < kNumThreads; i++) {
    rt::Thread([&, tid = i] {
      BatchReq batch_reqs[kNumProxies];
      uint32_t batch_spaces[kNumProxies];
      std::fill(batch_spaces, batch_spaces + kNumProxies, kBatchSize);

      while (true) {
        for (const auto &req : reqs[tid]) {
          auto &batch_space = batch_spaces[req.proxy_id];
          auto &batch_req = batch_reqs[req.proxy_id];
          batch_req.keys[--batch_space] = req.key;
          if (!batch_space) {
            hashtable_get(tid, batch_req, req.proxy_id);
            cnts[tid].cnt += kBatchSize;
            batch_space = kBatchSize;
          }
        }
      }
    }).Detach();
  }

  uint64_t old_sum = 0;
  uint64_t old_us = microtime();
  while (true) {
    timer_sleep(kPrintIntervalUS);
    auto us = microtime();
    uint64_t sum = 0;
    for (uint32_t i = 0; i < kNumThreads; i++) {
      sum += ACCESS_ONCE(cnts[i].cnt);
    }
    std::cout << us - old_us << " " << sum - old_sum << std::endl;
    old_sum = sum;
    old_us = us;
  }
}

void init_tcp() {
  for (uint32_t i = 0; i < kNumProxies; i++) {
    netaddr raddr = {.ip = kProxyIps[i], .port = kProxyPort};
    for (uint32_t j = 0; j < kNumThreads; j++) {
      conns[i][j] =
          rt::TcpConn::DialAffinity(j % rt::RuntimeMaxCores(), raddr);
      delay_us(1000);
      BUG_ON(!conns[i][j]);
    }
  }
}

void do_work() {
  std::vector<Req> reqs[kNumThreads];
  init_tcp();
  gen_reqs(reqs);
  benchmark(reqs);
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

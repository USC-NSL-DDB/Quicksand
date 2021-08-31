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
constexpr uint32_t kNumProxys = 2;
constexpr uint32_t kProxyPort = 10086;
constexpr uint32_t kBatchSize = 2;

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

void init(Test::DSHashTable *hash_table) {
  std::vector<rt::Thread> threads;
  constexpr uint32_t kNumThreads = 400;
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
        hash_table->put(key, val);
      }
    });
  }
  for (auto &thread : threads) {
    thread.Join();
  }
}

class Proxy {
public:
  Proxy(Test::DSHashTable::Cap cap) : hash_table_(cap) {}

  void run_loop() {
    netaddr laddr = {.ip = 0, .port = kProxyPort};
    auto *queue = rt::TcpQueue::Listen(laddr, 128);
    rt::TcpConn *c;
    while ((c = queue->Accept())) {
      rt::Thread([&, c] { handle(c); }).Detach();
    }
  }

  void handle(rt::TcpConn *c) {
    while (true) {
      BatchReq batch_req;
      BUG_ON(c->ReadFull(&batch_req, sizeof(batch_req)) <= 0);
      BatchResp batch_resp;
      for (uint32_t i = 0; i < kBatchSize; i++) {
        auto optional_v = hash_table_.get(batch_req.keys[i]);
        batch_resp.found[i] = optional_v.has_value();
        if (batch_resp.found[i]) {
          batch_resp.val[i] = *optional_v;
        }
      }
      BUG_ON(c->WriteFull(&batch_resp, sizeof(batch_resp)) < 0);
    }
  }

private:
  Test::DSHashTable hash_table_;
};

void do_work() {
  Test::DSHashTable hash_table;
  std::cout << "start initing..." << std::endl;
  init(&hash_table);
  std::cout << "finish initing..." << std::endl;
  
  std::vector<nu::Future<void>> futures;
  RemObj<Proxy> proxies[kNumProxys];
  for (uint32_t i = 0; i < kNumProxys; i++) {
    proxies[i] = RemObj<Proxy>::create_pinned(hash_table.get_cap());
    futures.emplace_back(proxies[i].run_async(&Proxy::run_loop));
  }

  futures.front().get();
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

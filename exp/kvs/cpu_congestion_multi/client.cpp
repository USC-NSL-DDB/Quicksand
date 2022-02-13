#include <algorithm>
#include <atomic>
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
#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"
#include "nu/utils/rcu_hash_map.hpp"
#include "nu/utils/perf.hpp"

using namespace nu;

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr double kLoadFactor = 0.30;
constexpr uint32_t kPrintIntervalUS = 1000 * 1000;
constexpr uint32_t kNumProxies = 17;
constexpr uint32_t kProxyIps[] = {
    MAKE_IP_ADDR(18, 18, 1, 2),  MAKE_IP_ADDR(18, 18, 1, 5),
    MAKE_IP_ADDR(18, 18, 1, 7),  MAKE_IP_ADDR(18, 18, 1, 8),
    MAKE_IP_ADDR(18, 18, 1, 13), MAKE_IP_ADDR(18, 18, 1, 15),
    MAKE_IP_ADDR(18, 18, 1, 17), MAKE_IP_ADDR(18, 18, 1, 19),
    MAKE_IP_ADDR(18, 18, 1, 21), MAKE_IP_ADDR(18, 18, 1, 23),
    MAKE_IP_ADDR(18, 18, 1, 25), MAKE_IP_ADDR(18, 18, 1, 27),
    MAKE_IP_ADDR(18, 18, 1, 29), MAKE_IP_ADDR(18, 18, 1, 31),
    MAKE_IP_ADDR(18, 18, 1, 33), MAKE_IP_ADDR(18, 18, 1, 35),
    MAKE_IP_ADDR(18, 18, 1, 37)};
constexpr static netaddr kClientAddrs[] = {
    {.ip = MAKE_IP_ADDR(18, 18, 1, 6), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 10), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 11), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 12), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 14), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 16), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 18), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 20), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 22), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 24), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 26), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 28), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 30), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 32), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 34), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 36), .port = 9000},
    {.ip = MAKE_IP_ADDR(18, 18, 1, 38), .port = 9000},
};
constexpr uint32_t kProxyPort = 10086;
constexpr uint32_t kNumThreads = 250;
constexpr double kTargetMops = 170;
constexpr uint32_t kWarmupUs = 1 * kOneSecond;
constexpr uint32_t kDurationUs = 60 * kOneSecond;
constexpr static uint64_t kTimeSeriesIntervalUs = 10 * 1000;
constexpr uint32_t kPowerNumShards = 16;
constexpr uint32_t kNumBucketsPerShard = 32768;

struct MemcachedPerfThreadState : nu::PerfThreadState {
  MemcachedPerfThreadState(uint32_t _tid)
      : tid(_tid), rd(), gen(rd()), dist_char('A', 'z'),
        dist_proxy_id(0, kNumProxies - 1) {}

  uint32_t tid;
  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<char> dist_char;
  std::uniform_int_distribution<int> dist_proxy_id;
};

rt::TcpConn *conns[kNumProxies][kNumThreads];

RCUHashMap<uint32_t, uint32_t> shard_id_to_proxy_id_map_;

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
  uint32_t shard_id;
};

struct PerfReq: nu::PerfRequest {
  Req req;
};

struct Resp {
  uint32_t latest_shard_ip;
  bool found;
  Val val;
};

namespace nu {

class Test {
public:
  constexpr static auto kFarmHashKeytoU64 = [](const Key &key) {
    return util::Hash64(key.data, kKeyLen);
  };
  using DSHashTable =
      DistributedHashTable<Key, Val, decltype(kFarmHashKeytoU64),
                           std::equal_to<Key>, kNumBucketsPerShard>;
  constexpr static size_t kNumPairs =
      (1 << kPowerNumShards) * DSHashTable::kNumBucketsPerShard * kLoadFactor;
};
} // namespace nu

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = dist(mt);
  }
}

void hashtable_get(uint32_t tid, const Req &req) {
  auto *proxy_id_ptr = shard_id_to_proxy_id_map_.get(req.shard_id);
  auto proxy_id = (!proxy_id_ptr) ? 0 : *proxy_id_ptr;
  BUG_ON(conns[proxy_id][tid]->WriteFull(&req, sizeof(req)) < 0);
  Resp resp;
  BUG_ON(conns[proxy_id][tid]->ReadFull(&resp, sizeof(resp)) <= 0);
  if (resp.latest_shard_ip) {
    auto proxy_ip_ptr = std::find(std::begin(kProxyIps), std::end(kProxyIps),
                                  resp.latest_shard_ip);
    BUG_ON(proxy_ip_ptr == std::end(kProxyIps));
    uint32_t proxy_id = proxy_ip_ptr - std::begin(kProxyIps);
    shard_id_to_proxy_id_map_.put(req.shard_id, proxy_id);
  }
}

class MemcachedPerfAdapter : public nu::PerfAdapter {
public:
  std::unique_ptr<nu::PerfThreadState> create_thread_state() {
    static std::atomic<uint32_t> num_threads = 0;
    uint32_t tid = num_threads++;
    return std::make_unique<MemcachedPerfThreadState>(tid);
  }

  std::unique_ptr<nu::PerfRequest> gen_req(nu::PerfThreadState *perf_state) {
    auto *state = reinterpret_cast<MemcachedPerfThreadState *>(perf_state);

    auto perf_req = std::make_unique<PerfReq>();
    random_str(state->dist_char, state->gen, kKeyLen, perf_req->req.key.data);
    perf_req->req.shard_id =
        Test::DSHashTable::get_shard_idx(perf_req->req.key, kPowerNumShards);

    return perf_req;
  }

  bool serve_req(nu::PerfThreadState *perf_state,
                 const nu::PerfRequest *perf_req) {
    auto *state = reinterpret_cast<MemcachedPerfThreadState *>(perf_state);
    auto *req = reinterpret_cast<const PerfReq *>(perf_req);
    hashtable_get(state->tid, req->req);

    return true;
  }
};

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

void register_callback() {
  netaddr laddr{.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  netaddr raddr{.ip = MAKE_IP_ADDR(18, 18, 1, 2), .port = nu::Migrator::kPort};
  auto *c = rt::TcpConn::Dial(laddr, raddr);
  BUG_ON(!c);
  uint8_t type = nu::kRegisterCallBack;
  BUG_ON(c->WriteFull(&type, sizeof(type)) != sizeof(type));
  rt::Thread t([c] {
    bool dummy;
    BUG_ON(c->ReadFull(&dummy, sizeof(dummy)) != sizeof(dummy));
    std::cout << "microtime() = " << microtime() << std::endl;
  });
}

void do_work() {
  register_callback();
  init_tcp();

  MemcachedPerfAdapter memcached_perf_adapter;
  nu::Perf perf(memcached_perf_adapter);
  perf.run_multi_clients(std::span(kClientAddrs), kNumThreads,
                         kTargetMops / std::size(kClientAddrs), kDurationUs,
                         kWarmupUs);
  std::cout << "real_mops, avg_lat, 50th_lat, 90th_lat, 95th_lat, 99th_lat, "
               "99.9th_lat"
            << std::endl;
  std::cout << perf.get_real_mops() << " " << perf.get_average_lat() << " "
            << perf.get_nth_lat(50) << " " << perf.get_nth_lat(90) << " "
            << perf.get_nth_lat(95) << " " << perf.get_nth_lat(99) << " "
            << perf.get_nth_lat(99.9) << std::endl;
  {
    auto timeseries_vec =
        perf.get_timeseries_nth_lats(kTimeSeriesIntervalUs, 99);
    std::ofstream ofs("timeseries");
    for (auto [absl_us, us, lat] : timeseries_vec) {
      ofs << absl_us << " " << us << " " << lat << std::endl;
    }
  }
  {
    auto timeseries_vec =
        perf.get_timeseries_nth_lats(kTimeSeriesIntervalUs, 99.9);
    std::ofstream ofs("timeseries-9");
    for (auto [absl_us, us, lat] : timeseries_vec) {
      ofs << absl_us << " " << us << " " << lat << std::endl;
    }
  }
  {
    auto timeseries_vec =
        perf.get_timeseries_nth_lats(kTimeSeriesIntervalUs, 99.99);
    std::ofstream ofs("timeseries-99");
    for (auto [absl_us, us, lat] : timeseries_vec) {
      ofs << absl_us << " " << us << " " << lat << std::endl;
    }
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

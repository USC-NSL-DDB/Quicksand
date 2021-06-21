#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
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

constexpr auto kIPServer = MAKE_IP_ADDR(18, 18, 1, 4);

Runtime::Mode mode;
bool use_local;

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

constexpr static auto kFarmHashKeytoU64 = [](const Key &key) {
  return util::Hash64(key.data, kKeyLen);
};
using DSHashTable = DistributedHashTable<Key, Val, decltype(kFarmHashKeytoU64)>;

namespace nu {
// A hack for reading DistributedHashTable's private members.
struct Test {
  constexpr static size_t kTotalNumBuckets =
      DSHashTable::kNumShards * DSHashTable::kNumBucketsPerShard;
  constexpr static size_t kNumPairs = kTotalNumBuckets * kLoadFactor;

  Test() {}
  uint64_t get_mem_usage() {
    uint64_t total_mem_usage = 0;
    std::function<bool(HeapHeader *const &)> fn =
        [&total_mem_usage](HeapHeader *const &heap_header) {
          auto &heap_slab = heap_header->slab;
          total_mem_usage += reinterpret_cast<uint8_t *>(heap_slab.get_base()) -
                             reinterpret_cast<uint8_t *>(heap_header) +
                             heap_slab.get_usage();
          return true;
        };
    Runtime::heap_manager->heap_statuses_->for_each(fn);
    return total_mem_usage;
  }
};
} // namespace nu

using LocalHashTable =
    SyncHashMap<Test::kTotalNumBuckets, Key, Val, decltype(kFarmHashKeytoU64),
                std::equal_to<Key>, std::allocator<std::pair<const Key, Val>>,
                SpinLock>;

enum Op { PUT, GET, DELETE };

struct Command {
  Op op;
  Key key;
  std::optional<Val> val;

  Command(Op _op, Key _key, std::optional<Val> _val = std::nullopt)
      : op(_op), key(_key), val(_val) {}
};

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = dist(mt);
  }
}

void gen_commands(std::vector<Command> *commands) {
  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i, commands_pt = &commands[i]] {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist('A', 'z');
      auto num_pairs = Test::kNumPairs / kNumThreads;
      for (size_t j = 0; j < num_pairs; j++) {
        Key key;
        Val val;
        random_str(dist, mt, kKeyLen, key.data);
        random_str(dist, mt, kValLen, val.data);
        commands_pt->emplace_back(PUT, key, val);
      }
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }
}

uint64_t run_on_local_hashtable(std::vector<Command> *commands) {
  auto padding =
      (2ULL << bsr_64(sizeof(LocalHashTable))) - sizeof(LocalHashTable);
  auto mem_usage_start = Runtime::runtime_slab.get_usage();
  auto *local_hashtable = new LocalHashTable();

  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i, commands_pt = &commands[i]] {
      for (auto &cmd : *commands_pt) {
        switch (cmd.op) {
        case PUT:
          local_hashtable->put(cmd.key, *cmd.val);
          break;
        default:
          BUG();
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }

  auto mem_usage_end = Runtime::runtime_slab.get_usage();

  return mem_usage_end - mem_usage_start - padding;
}

uint64_t run_on_dis_hashtable(std::vector<Command> *commands) {
  // To make the mem usage counting work, we must only use one remote server.
  auto test = RemObj<nu::Test>::create();
  auto mem_usage_start = test.run(&nu::Test::get_mem_usage);
  auto *dis_hashtable = new DSHashTable();

  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i, commands_pt = &commands[i]] {
      for (auto &cmd : *commands_pt) {
        switch (cmd.op) {
        case PUT:
          dis_hashtable->put(cmd.key, *cmd.val);
          break;
        default:
          BUG();
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }

  auto mem_usage_end = test.run(&nu::Test::get_mem_usage);
  return mem_usage_end - mem_usage_start;
}

void do_work() {
  std::cout << "gen_commands..." << std::endl;
  std::vector<Command> commands[kNumThreads];
  gen_commands(commands);
  if (use_local) {
    std::cout << "run_on_local_hashtable..." << std::endl;
    auto local_hashtable_mem_usage = run_on_local_hashtable(commands);
    std::cout << local_hashtable_mem_usage << std::endl;
  } else {
    std::cout << "run_on_dis_hashtable..." << std::endl;
    auto dis_hashtable_mem_usage = run_on_dis_hashtable(commands);
    std::cout << dis_hashtable_mem_usage << std::endl;
  }
}

int main(int argc, char **argv) {
  int ret;
  std::string mode_str, local_str;

  if (argc < 4) {
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

  local_str = std::string(argv[3]);
  if (local_str == "L") {
    use_local = true;
  } else if (local_str == "R") {
    use_local = false;
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
  std::cerr << "usage: [cfg_file] CLT/SRV/CTL L/R" << std::endl;
  return -EINVAL;
}

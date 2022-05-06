#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <utility>
#include <vector>
#include <set>
#include <cereal/types/optional.hpp>
#include <cereal/types/string.hpp>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/dis_hash_table.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"
#include "nu/utils/trace_logger.hpp"

using namespace nu;

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr double kLoadFactor = 0.20;
constexpr uint32_t kNumThreads = 100;
constexpr uint32_t kDeletePercentage = 10;

constexpr auto kIPServer = MAKE_IP_ADDR(18, 18, 1, 4);

Runtime::Mode mode;
bool use_local;

struct Key {
  char data[kKeyLen];

  bool operator==(const Key &o) const {
    return __builtin_memcmp(data, o.data, kKeyLen) == 0;
  }

  bool operator<(const Key &o) const {
    for (uint32_t i = 0; i < kKeyLen; i++) {
      if (data[i] != o.data[i]) {
        return data[i] < o.data[i];
      }
    }
    return false;
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
      (1 << DSHashTable::kDefaultPowerNumShards) *
      DSHashTable::kNumBucketsPerShard;
  constexpr static size_t kNumPairs = kTotalNumBuckets * kLoadFactor;

  Test() {}
  uint64_t get_mem_usage() { return Runtime::heap_manager->get_mem_usage(); }
};
} // namespace nu

using LocalHashTable =
    SyncHashMap<Test::kTotalNumBuckets, Key, Val, decltype(kFarmHashKeytoU64),
                std::equal_to<Key>, std::allocator<std::pair<const Key, Val>>,
                SpinLock>;

enum Op { PUT, DELETE };

struct Command {
  Op op;
  Key key;
  std::optional<Val> val;

  Command(Op _op, Key _key, std::optional<Val> _val = std::nullopt)
      : op(_op), key(_key), val(_val) {}
};

Op random_op(auto &dist, auto &mt) {
  uint32_t n = dist(mt) % 100;
  if (n < kDeletePercentage) {
    return DELETE;
  }
  return PUT;
}

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = dist(mt);
  }
}

void gen_commands(std::vector<Command> *commands) {
  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, tid = i, commands_pt = &commands[i]] {
      std::set<Key> set_pt;
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist('A', 'z');
      auto num_pairs = Test::kNumPairs / kNumThreads;
      while (set_pt.size() < num_pairs) {
        auto op = random_op(dist, mt);
        Key key;
        Val val;
        std::set<Key>::iterator iter;

        switch (op) {
	case PUT:
          random_str(dist, mt, kKeyLen, key.data);
          random_str(dist, mt, kValLen, val.data);
          commands_pt->emplace_back(PUT, key, val);
          set_pt.emplace(key);
          break;
        case DELETE:
          random_str(dist, mt, kKeyLen, key.data);
          iter = set_pt.lower_bound(key);
          if (unlikely(iter == set_pt.end())) {
            break;
          }
          key = *iter;
          commands_pt->emplace_back(DELETE, key);
          set_pt.erase(iter);
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
        case DELETE:
          BUG_ON(!local_hashtable->remove(cmd.key));
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
  auto test = Proclet<nu::Test>::create();
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
	case DELETE:
          BUG_ON(!dis_hashtable->remove(cmd.key));
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
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

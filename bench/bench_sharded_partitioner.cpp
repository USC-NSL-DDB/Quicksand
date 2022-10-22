#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_partitioner.hpp"

constexpr uint32_t kRunTimes = 1;
constexpr uint32_t kNumElements = 256 << 20;
constexpr uint32_t kNumThreads = nu::kNumCores - 2;
constexpr uint32_t kKeyLen = 10;
constexpr uint32_t kValLen = 90;

struct Key {
  uint8_t data[kKeyLen];

  Key() {}

  Key(uint64_t num) {
    static_assert(kKeyLen >= sizeof(num));
    *reinterpret_cast<uint64_t *>(data) = num;
    std::reverse(data, data + sizeof(num));
    memset(data + sizeof(num), 0, kKeyLen - sizeof(num));
  }

  bool operator<(const Key &o) const {
    return std::memcmp(data, o.data, kKeyLen) < 0;
  }

  bool operator>(const Key &o) const {
    return std::memcmp(data, o.data, kKeyLen) > 0;
  }

  bool operator==(const Key &o) const {
    return std::memcmp(data, o.data, kKeyLen) == 0;
  }

  bool operator<=(const Key &o) const {
    return std::memcmp(data, o.data, kKeyLen) <= 0;
  }

  Key &operator+=(const uint64_t offset) {
    std::reverse(data, data + sizeof(offset));
    *reinterpret_cast<uint64_t *>(data) += offset;
    std::reverse(data, data + sizeof(offset));
    return *this;
  }
};

struct Val {
  uint8_t data[kValLen];

  Val() {}

  Val(uint64_t num) {
    static_assert(kValLen >= sizeof(num));
    *reinterpret_cast<uint64_t *>(data) = num;
    std::reverse(data, data + sizeof(num));
    memset(data + sizeof(num), 0, kValLen - sizeof(num));
  }

  bool operator<(const Val &) const { return false; }
};

class Worker {
 public:
  Worker(nu::ShardedPartitioner<Key, Val> sp) : sp_(std::move(sp)) {}

  void do_work(uint32_t wid) {
    auto num_elems_per_th = kNumElements / kNumThreads;
    for (uint32_t i = 0; i < num_elems_per_th; i++) {
      sp_.emplace(wid * num_elems_per_th + i, i);
    }
  }

 private:
  nu::ShardedPartitioner<Key, Val> sp_;
};

class Bench {
 public:
  Bench() {
    for (uint32_t i = 0; i < kRunTimes; i++) {
      std::cout << "Running No." << i << " time..." << std::endl;
      single_thread_std_vector();
      single_thread_no_partition();
      single_thread_perfect_partition();
      multi_threads_no_partition();
      multi_threads_perfect_partition();
      // TODO: add more.
    }
  }

  void single_thread_std_vector() {
    std::cout << "\tRunning single-thread-std-vector bench..." << std::endl;

    nu::RuntimeSlabGuard slab;
    std::vector<std::pair<Key, Val>> v;

    auto t0 = microtime();
    for (uint32_t i = 0; i < kNumElements; i++) {
      v.emplace_back(i, i);
    }
    auto t1 = microtime();
    auto mops = static_cast<double>(kNumElements) / (t1 - t0);
    auto bw = mops * sizeof(std::pair<Key, Val>);
    std::cout << "\t\tstd::vector: " << t1 - t0 << " us, " << mops << " MOPS, "
              << bw << " MB/s" << std::endl;
  }

  void single_thread_no_partition() {
    std::cout << "\tRunning single-thread-no-partition bench..." << std::endl;
    auto sp = nu::make_sharded_partitioner<Key, Val>();
    single_thread(&sp);
  }

  void single_thread_perfect_partition() {
    std::cout << "\tRunning single-thread-perfect-partition bench..."
              << std::endl;
    auto sp = nu::make_sharded_partitioner<Key, Val>(
        kNumElements, 0, [](Key &x, uint64_t offset) { x += offset; });
    single_thread(&sp);
  }

  void single_thread(nu::ShardedPartitioner<Key, Val> *sp_ptr) {
    auto t0 = microtime();
    for (uint32_t i = 0; i < kNumElements; i++) {
      sp_ptr->emplace(i, i);
    }
    auto t1 = microtime();
    auto mops = static_cast<double>(kNumElements) / (t1 - t0);
    auto bw = mops * sizeof(std::pair<Key, Val>);
    std::cout << "\t\tShardedPartitioner: " << t1 - t0 << " us, " << mops
              << " MOPS, " << bw << " MB/s" << std::endl;
  }

  void multi_threads_no_partition() {
    std::cout << "\tRunning multi-threads-no-partition bench..." << std::endl;
    auto sp = nu::make_sharded_partitioner<Key, Val>();
    multi_threads(&sp);
  }

  void multi_threads_perfect_partition() {
    std::cout << "\tRunning multi-threads-perfect-partition bench..."
              << std::endl;
    auto sp = nu::make_sharded_partitioner<Key, Val>(
        kNumElements, 0, [](Key &x, uint64_t offset) { x += offset; });
    multi_threads(&sp);
  }

  void multi_threads(nu::ShardedPartitioner<Key, Val> *sp_ptr) {
    std::vector<nu::Proclet<Worker>> workers;

    for (uint32_t i = 0; i < kNumThreads; i++) {
      workers.emplace_back(nu::make_proclet<Worker>(*sp_ptr));
    }

    std::vector<nu::Future<void>> futures;
    for (uint32_t i = 0; i < kNumThreads; i++) {
      futures.emplace_back(workers[i].run_async(&Worker::do_work, i));
    }

    auto t0 = microtime();
    for (auto &future : futures) {
      future.get();
    }
    auto t1 = microtime();
    auto mops = static_cast<double>(kNumElements) / (t1 - t0);
    auto bw = mops * sizeof(std::pair<Key, Val>);
    std::cout << "\t\tShardedPartitioner: " << t1 - t0 << " us, " << mops
              << " MOPS, " << bw << " MB/s" << std::endl;
  }
};

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv,
                               [](int, char **) { nu::make_proclet<Bench>(); });
}

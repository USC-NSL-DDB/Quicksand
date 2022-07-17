#include <algorithm>
#include <cereal/types/string.hpp>
#include <functional>
#include <iostream>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_pair_collect.hpp"

constexpr uint32_t kNumElements = 16 << 20;

class Worker {
 public:
  Worker(nu::ShardedPairCollection<int, std::string> sc) : sc_(std::move(sc)) {}

  void emplace(uint32_t start, uint32_t end) {
    for (uint32_t i = start; i < end; i++) {
      auto str = std::to_string(i);
      sc_.emplace(i, str);
    }
    sc_.flush();
  }

  void mutate() {
    sc_.for_all(
        +[](std::pair<const int, std::string> &p, char new_c) {
          p.second += new_c;
        },
        ' ');
  }

 private:
  nu::ShardedPairCollection<int, std::string> sc_;
};

bool run_test(nu::ShardedPairCollection<int, std::string> *sc) {
  auto p0 = make_proclet<Worker>(*sc);
  auto p1 = make_proclet<Worker>(*sc);

  auto f0 = p0.run_async(&Worker::emplace, 0, kNumElements / 2);
  auto f1 = p1.run_async(&Worker::emplace, kNumElements / 2, kNumElements);
  f0.get();
  f1.get();

  auto f2 = p0.run_async(&Worker::mutate);
  auto f3 = p1.run_async(&Worker::mutate);
  f2.get();
  f3.get();

  nu::RuntimeSlabGuard g;

  auto c = sc->collect();
  auto &v = c.unwrap().get_data();
  sort(v.begin(), v.end());

  std::vector<std::pair<int, std::string>> expected_v;
  for (uint32_t i = 0; i < kNumElements; i++) {
    expected_v.emplace_back(i, std::string(std::to_string(i) + "  "));
  }

  return v == expected_v;
}

bool run_test_no_hint() {
  auto sc = nu::make_sharded_pair_collection<int, std::string>();
  return run_test(&sc);
}

bool run_test_with_hint() {
  // Intentionally use a very bad hint.
  auto sc = nu::make_sharded_pair_collection<int, std::string>(
      /* num = */ kNumElements, /* estimated_min_key = */ kNumElements,
      /* key_inc_fn = */ [](int &k, uint64_t offset) { k += offset; });
  return run_test(&sc);
}

bool run_all_tests() {
  return run_test_no_hint() && run_test_with_hint();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    if (run_all_tests()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}

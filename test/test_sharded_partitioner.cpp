#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_partitioner.hpp"

constexpr uint32_t kNumElements = 16 << 20;

class Worker {
 public:
  Worker(nu::ShardedPartitioner<int, std::string> sp) : sp_(std::move(sp)) {}

  void emplace(uint32_t start, uint32_t end) {
    for (uint32_t i = start; i < end; i++) {
      auto str = std::to_string(i);
      sp_.emplace(i, str);
    }
    sp_.flush();
  }

  void mutate() {
    sp_.for_all(
        +[](const int &key, std::string &val, char new_c) { val += new_c; },
        ' ');
  }

 private:
  nu::ShardedPartitioner<int, std::string> sp_;
};

bool run_test(nu::ShardedPartitioner<int, std::string> *sp) {
  {
    auto p0 = make_proclet<Worker>(*sp);
    auto p1 = make_proclet<Worker>(*sp);

    auto f0 = p0.run_async(&Worker::emplace, 0, kNumElements / 2);
    auto f1 = p1.run_async(&Worker::emplace, kNumElements / 2, kNumElements);
    f0.get();
    f1.get();

    auto f2 = p0.run_async(&Worker::mutate);
    auto f3 = p1.run_async(&Worker::mutate);
    f2.get();
    f3.get();
  }

  nu::RuntimeSlabGuard g;

  auto c = sp->collect();
  auto &pc = c.unwrap();
  std::vector<std::pair<int, std::string>> v(pc.data(), pc.data() + pc.size());
  sort(v.begin(), v.end());

  std::vector<std::pair<int, std::string>> expected_v;
  for (uint32_t i = 0; i < kNumElements; i++) {
    expected_v.emplace_back(i, std::string(std::to_string(i) + "  "));
  }

  if (v != expected_v) {
    return false;
  }

  auto sealed = nu::to_sealed_ds(std::move(*sp));
  v.clear();
  for (const auto &s : sealed) {
    v.emplace_back(s);
  }
  sort(v.begin(), v.end());
  if (v != expected_v) {
    return false;
  }

  v.clear();
  for (auto it = sealed.crbegin(); it != sealed.crend(); ++it) {
    v.emplace_back(*it);
  }
  sort(v.begin(), v.end());
  if (v != expected_v) {
    return false;
  }

  return true;
}

bool run_test_no_hint() {
  auto sp = nu::make_sharded_partitioner<int, std::string>();
  return run_test(&sp);
}

bool run_test_with_hint() {
  // Intentionally uses a very bad hint.
  auto sp = nu::make_sharded_partitioner<int, std::string>(
      /* num = */ kNumElements, /* estimated_min_key = */ kNumElements,
      /* key_inc_fn = */ [](int &k, uint64_t offset) { k += offset; });
  return run_test(&sp);
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

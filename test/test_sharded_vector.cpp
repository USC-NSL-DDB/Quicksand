#include <iostream>
#include <string>
#include <vector>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_vector.hpp"

using namespace nu;

bool test_push_and_set() {
  constexpr uint32_t kSize = 50 << 20;
  auto vec = make_sharded_vector<int, std::false_type>(40 << 20);

  for (size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
  }

  {
    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    int i = 0;
    for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
      if (*it != i) {
        return false;
      }
    }
    vec = nu::to_unsealed_ds(std::move(sealed_vec));
  }

  for (size_t i = 0; i < kSize; i++) {
    vec.set(i, 2 * i);
  }

  {
    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    int i = 0;
    for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
      if (*it != 2 * i) {
        return false;
      }
    }
  }

  return true;
}

bool test_vec_clear() {
  constexpr uint32_t kSize = 10 << 20;
  auto vec = make_sharded_vector<int, std::false_type>();

  if (!vec.empty()) {
    return false;
  }

  for (size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
  }

  if (vec.empty()) {
    return false;
  }
  vec.clear();
  if (!vec.empty()) {
    return false;
  }

  return true;
}

bool test_for_all() {
  constexpr int kSize = 10 << 20;
  auto vec = make_sharded_vector<int, std::false_type>();

  for (int i = 0; i < kSize; i++) {
    vec.push_back(i);
  }

  vec.for_all(+[](const std::size_t &idx, int &val) { val *= 2; });

  {
    auto sealed_vec = nu::to_sealed_ds(std::move(vec));
    int i = 0;
    for (auto it = sealed_vec.cbegin(); it != sealed_vec.cend(); ++it, ++i) {
      if (*it != 2 * i) {
        return false;
      }
    }
  }

  return true;
}

template <typename T, typename LL>
class BackInserterWorker {
 public:
  static const std::size_t kNumElements = 1'000'000;
  static const std::size_t kNumWorkers = 10;

  BackInserterWorker(nu::VectorBackInserter<T, LL> inserter, std::size_t wid)
      : inserter_(std::move(inserter)), wid_(wid) {
    if (wid < kNumWorkers - 1) {
      auto next_wid = wid + 1;
      nu::make_proclet<BackInserterWorker<int, std::false_type>>(
          inserter_.split(next_wid), next_wid);
    }

    do_work();
  }

  void do_work() {
    for (std::size_t i = 0; i < kNumElements; ++i) {
      inserter_.push_back(wid_ * kNumElements + i);
    }
  }

 private:
  nu::VectorBackInserter<T, LL> inserter_;
  std::size_t wid_;
};

bool test_back_inserter() {
  auto num_workers = BackInserterWorker<int, std::false_type>::kNumWorkers;
  auto num_elems = BackInserterWorker<int, std::false_type>::kNumElements;

  auto v = nu::make_sharded_vector<int, std::false_type>();
  {
    auto inserter = v.back_inserter();
    auto worker = nu::make_proclet<BackInserterWorker<int, std::false_type>>(
        std::move(inserter), 0);
  }

  // blocks until insertion is done
  auto sealed = nu::to_sealed_ds(std::move(v));

  auto expected_len = num_workers * num_elems;
  if (sealed.size() != expected_len) {
    return false;
  }

  int expected = 0;
  for (const auto elem : sealed) {
    if (elem != expected) {
      return false;
    }
    ++expected;
  }

  return true;
}

bool run_test() {
  if (!test_push_and_set()) {
    return false;
  }
  if (!test_vec_clear()) {
    return false;
  }
  if (!test_for_all()) {
    return false;
  }
  if (!test_back_inserter()) {
    return false;
  }

  return true;
}

void do_work() {
  if (run_test()) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_map.hpp"

using namespace nu;

bool test_insertion() {
  auto sm = make_sharded_map<int, int, false>();
  for (int i = 0; i < 100'000; i++) {
    sm.emplace(i, i);
  }
  for (int i = 0; i < 100'000; i++) {
    if (sm[i] != i) {
      return false;
    }
  }
  return true;
}

bool test_size_and_clear() {
  std::size_t target_size = 200'000'000;

  auto sm = make_sharded_map<std::size_t, std::size_t, false>();
  if (sm.size() != 0) return false;

  for (std::size_t i = 0; i < target_size; i++) {
    sm.emplace(i, i);
  }
  if (sm.size() != target_size) return false;

  sm.clear();
  if (sm.size() != 0) return false;

  return true;
}

bool run_test() { return test_insertion() && test_size_and_clear(); }

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

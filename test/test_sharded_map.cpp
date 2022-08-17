#include <cereal/types/string.hpp>
#include <random>
#include <string>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_map.hpp"

using namespace nu;

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('A', 'z');

std::string random_str(uint32_t len) {
  std::string str = "";
  for (uint32_t i = 0; i < len; i++) {
    str += dist(mt);
  }
  return str;
}

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

bool test_for_all_ul() {
  std::size_t target_size = 200'000'000;

  auto sm = make_sharded_map<std::size_t, std::size_t, false>();
  for (std::size_t i = 0; i < target_size; i++) {
    sm.emplace(i, i);
  }

  sm.for_all(
      +[](const std::size_t &k, std::size_t &v, int multiplier) {
        v = v * multiplier;
      },
      2);

  if (sm.size() != target_size) return false;
  for (std::size_t i = 0; i < target_size; i++) {
    if (sm[i] != i * 2) {
      return false;
    }
  }

  return true;
}

bool test_for_all_str() {
  std::size_t target_size = 200'000'000;

  auto sm = make_sharded_map<std::string, std::string, false>();
  for (std::size_t i = 0; i < target_size; i++) {
    sm.emplace(std::to_string(i), random_str(128));
  }
  sm.for_all(+[](const std::string &k, std::string &v) {
    std::transform(v.begin(), v.end(), v.begin(),
                   [](auto c) { return std::toupper(c); });
  });
  for (std::size_t i = 0; i < target_size; i++) {
    auto s = sm[std::to_string(i)];
    if (!std::all_of(s.begin(), s.end(),
                     [](unsigned char c) { return std::isupper(c); })) {
      return false;
    }
  }

  return true;
}

bool test_for_all() { return test_for_all_ul() && test_for_all_str(); }

bool run_test() {
  return test_insertion() && test_size_and_clear() && test_for_all();
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

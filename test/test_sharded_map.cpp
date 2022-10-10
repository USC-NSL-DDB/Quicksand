#include <iostream>
#include <random>
#include <string>

#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_map.hpp"

using namespace nu;

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('a', 'z');

std::string random_lowercase_str(uint32_t len) {
  std::string str = "";
  for (uint32_t i = 0; i < len; i++) {
    str += dist(mt);
  }
  return str;
}

bool test_insertion() {
  auto sm = make_sharded_map<int, int, std::false_type>();
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
  std::size_t target_size = 200'000;

  auto sm = make_sharded_map<std::size_t, std::size_t, std::false_type>();
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
  std::size_t target_size = 200'000;

  auto sm = make_sharded_map<std::size_t, std::size_t, std::false_type>();
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
  std::size_t target_size = 50'000;

  auto sm = make_sharded_map<std::string, std::string, std::false_type>();
  for (std::size_t i = 0; i < target_size; i++) {
    sm.emplace(std::to_string(i), random_lowercase_str(128));
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

bool test_iter() {
  auto sm = make_sharded_map<int, int, std::false_type>();
  for (int i = 0; i < 100'000; i++) {
    sm.emplace(i, i);
  }

  auto sealed_sm = to_sealed_ds(std::move(sm));
  int i = 0;
  for (auto iter = sealed_sm.cbegin(); iter != sealed_sm.cend(); ++iter, ++i) {
    auto [k, v] = *iter;
    if (k != i || v != i) return false;
  }

  --i;
  for (auto iter = sealed_sm.crbegin(); iter != sealed_sm.crend();
       ++iter, --i) {
    auto [k, v] = *iter;
    if (k != i || v != i) return false;
  }

  return true;
}

bool run_test() {
  return test_insertion() && test_size_and_clear() && test_for_all() &&
         test_iter();
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

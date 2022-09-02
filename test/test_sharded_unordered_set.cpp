#include <cereal/types/string.hpp>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sealed_ds.hpp"
#include "nu/sharded_unordered_set.hpp"

using namespace nu;

std::vector<std::string> make_test_str_vec(uint32_t size);
std::vector<std::string> kTestStrs = make_test_str_vec(100'000);

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

std::vector<std::string> make_test_str_vec(uint32_t size) {
  int test_str_len = 35;
  std::vector<std::string> vec(size);
  for (uint32_t i = 0; i < size; i++) {
    vec[i] = random_str(test_str_len);
  }
  return vec;
}

bool test_insertion() {
  std::size_t num_elems = 1'000'000;

  std::unordered_set<std::size_t> expected;
  std::unordered_set<std::size_t> iterated;
  auto s = make_sharded_unordered_set<std::size_t, std::false_type>();

  for (std::size_t i = 0; i < num_elems; ++i) {
    s.insert(i);
    expected.insert(i);
  }

  auto sealed_set = to_sealed_ds(std::move(s));
  for (auto it = sealed_set.cbegin(); it != sealed_set.cend(); ++it) {
    iterated.insert(*it);
  }

  return iterated == expected;
}

bool test_size() {
  std::size_t num_elems = 1'000'000;
  auto s = make_sharded_unordered_set<std::size_t, std::false_type>();

  if (s.size() != 0) return false;

  for (std::size_t i = 1; i <= num_elems; i++) {
    s.insert(i);
  }
  if (s.size() != num_elems) return false;

  for (std::size_t i = 1; i <= num_elems; i++) {
    s.insert(i);
  }
  if (s.size() != num_elems) return false;

  return true;
}

bool test_clear() {
  std::size_t num_elems = 1'000'000;
  auto s = make_sharded_unordered_set<std::size_t, std::false_type>();

  for (std::size_t i = 1; i <= num_elems; i++) {
    s.insert(i);
  }
  s.clear();

  return s.size() == 0;
}

bool run_test() { return test_insertion() && test_size() && test_clear(); }

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

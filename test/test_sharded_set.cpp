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
#include "nu/sharded_set.hpp"

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

template <typename T, typename LL>
bool test_insert_elems_ordering(std::vector<T> &elems) {
  std::set<T> expected_set;
  auto s = make_sharded_set<T, LL>();

  for (auto &elem : elems) {
    s.insert(elem);
    expected_set.insert(elem);
  }

  auto collected = s.collect();
  if (collected.unwrap().data().size() != expected_set.size()) return false;

  auto expected = expected_set.begin();
  auto actual = collected.unwrap().data().begin();
  while (actual != collected.unwrap().data().end() &&
         expected != expected_set.end()) {
    if (*actual != *expected) return false;
    actual++;
    expected++;
  }

  return true;
}

template <typename T, typename LL>
bool test_range_insert(T start, T end) {
  auto s = make_sharded_set<T, LL>();
  for (int i = start; i < end; i++) {
    s.insert(i);
  }
  auto collected = s.collect();
  int expected = start;
  for (auto &elem : collected.unwrap().data()) {
    if (expected != elem) return false;
    expected++;
  }
  return true;
}

template <typename T, typename LL>
bool test_reverse_range_insert(T start, T end) {
  auto s = make_sharded_set<T, LL>();
  for (int i = end; i >= start; i--) {
    s.insert(i);
  }
  auto collected = s.collect();
  int expected = start;
  for (auto &elem : collected.unwrap().data()) {
    if (expected != elem) return false;
    expected++;
  }
  return true;
}

bool test_insertion() {
  return test_range_insert<int, std::false_type>(0, 1'000'000);
}

bool test_ordering() {
  return test_reverse_range_insert<int, std::false_type>(0, 1'000'000) &&
         test_insert_elems_ordering<std::string, std::false_type>(kTestStrs);
}

bool test_size() {
  std::size_t num_elems = 1'000'000;
  auto s = make_sharded_set<std::size_t, std::false_type>();

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
  auto s = make_sharded_set<std::size_t, std::false_type>();

  for (std::size_t i = 1; i <= num_elems; i++) {
    s.insert(i);
  }
  s.clear();

  return s.size() == 0;
}

bool test_iter() {
  std::size_t num_elems = 1'000'000;

  std::set<std::size_t> expected;
  std::set<std::size_t> iterated;
  auto s = make_sharded_set<std::size_t, std::false_type>();

  for (std::size_t i = 0; i < num_elems; ++i) {
    s.insert(i);
    expected.insert(i);
  }

  auto sealed_set = to_sealed_ds(std::move(s));
  for (auto it = sealed_set.cbegin(); it != sealed_set.cend(); ++it) {
    iterated.insert(*it);
  }
  if (iterated != expected) return false;

  iterated.clear();

  for (auto it = sealed_set.crbegin(); it != sealed_set.crend(); ++it) {
    iterated.insert(*it);
  }
  if (iterated != expected) return false;

  return true;
}

bool run_test() {
  return test_insertion() && test_ordering() && test_size() && test_clear() &&
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

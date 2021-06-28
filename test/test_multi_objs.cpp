#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <base/compiler.h>
#include <net/ip.h>
}
#include <runtime.h>
#include <thread.h>

#include "nu/rem_obj.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;
bool passed = true;

template <int N> class Obj {
public:
  void set_vec_a(std::vector<int> vec) { a_ = vec; }
  void set_vec_b(std::vector<int> vec) { b_ = vec; }
  std::vector<int> plus() {
    std::vector<int> c;
    for (size_t i = 0; i < a_.size(); i++) {
      c.push_back(a_[i] + b_[i]);
    }
    return c;
  }

private:
  std::vector<int> a_;
  std::vector<int> b_;
};

template <int N> void spawn_threads(std::vector<rt::Thread> *threads) {
  auto fn = [&] {
    std::vector<int> a{N, N + 1, N + 2, N + 3};
    std::vector<int> b{N + 4, N + 5, N + 6, N + 7};

    auto rem_obj = RemObj<Obj<N>>::create();
    auto future_0 = rem_obj.run_async(&Obj<N>::set_vec_a, a);
    auto future_1 = rem_obj.run_async(&Obj<N>::set_vec_b, b);
    future_0.get();
    future_1.get();
    auto c = rem_obj.run(&Obj<N>::plus);

    for (size_t i = 0; i < a.size(); i++) {
      if (c[i] != a[i] + b[i]) {
        ACCESS_ONCE(passed) = false;
        break;
      }
    }
  };
  threads->emplace_back(std::move(fn));
  if constexpr (N > 1) {
    spawn_threads<N - 1>(threads);
  }
}

void do_work() {
  std::vector<rt::Thread> threads;
  spawn_threads<100>(&threads);

  for (auto &thread : threads) {
    thread.Join();
  }

  if (ACCESS_ONCE(passed)) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

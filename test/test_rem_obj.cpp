#include <algorithm>
#include <cereal/types/utility.hpp>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
}
#include <runtime.h>

#include "nu/proclet.hpp"
#include "nu/runtime.hpp"

using namespace nu;

Runtime::Mode mode;

class Obj {
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

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  // Intentionally test the async method.
  auto proclet_future = make_proclet_async<Obj>();
  auto proclet = std::move(proclet_future.get());

  auto future_0 = proclet.run_async(&Obj::set_vec_a, a);
  auto future_1 = proclet.run_async(&Obj::set_vec_b, b);
  future_0.get();
  future_1.get();

  auto tmp_obj = make_proclet<ErasedType>();
  bool match;
  // We can move a Proclet into/out of closure without updating the ref cnt.
  std::tie(proclet, match) = tmp_obj.run(
      +[](ErasedType &, Proclet<Obj> proclet, std::vector<int> a,
          std::vector<int> b) {
        auto c = proclet.run(&Obj::plus);
        for (size_t i = 0; i < a.size(); i++) {
          if (c[i] != a[i] + b[i]) {
            return std::make_pair(std::move(proclet), false);
          }
        }
        return std::make_pair(std::move(proclet), true);
      },
      std::move(proclet), a, b);
  passed &= match;

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

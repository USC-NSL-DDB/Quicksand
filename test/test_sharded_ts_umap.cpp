#include "nu/runtime.hpp"
#include "nu/sharded_ts_umap.hpp"

using namespace nu;

bool run_test() {
  auto sm = make_sharded_ts_umap<std::size_t, std::size_t>();
  sm.insert(1, 1);
  sm.apply_on(
      1,
      +[](std::pair<const std::size_t, std::size_t> &p, int delta) {
        p.second += delta;
      },
      2);
  auto optional = sm.find_data(1);
  if (!optional || *optional != 3) {
    return false;
  }
  if (!sm.erase(1)) {
    return false;
  }
  optional = sm.find_data(1);
  if (optional) {
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

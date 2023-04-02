#include "nu/runtime.hpp"
#include "nu/sharded_ts_umap.hpp"

using namespace nu;

constexpr uint32_t kNumElements = 1 << 20;

bool run_test() {
  auto sm = make_sharded_ts_umap<std::size_t, std::string>();
  sm.insert(1, std::to_string(1));
  sm.apply_on(
      1,
      +[](std::pair<const std::size_t, std::string> &p, std::string delta) {
        p.second += delta;
      },
      std::string(
          "2"));  // TODO: investigate the crash when passing a c-string "2".
  auto optional = sm.find_data(1);
  if (!optional || *optional != "12") {
    return false;
  }
  if (!sm.erase(1)) {
    return false;
  }
  optional = sm.find_data(1);
  if (optional) {
    return false;
  }

  for (std::size_t i = 0; i < kNumElements; i++) {
    sm.insert(i, std::to_string(i));
  }

  for (std::size_t i = 0; i < kNumElements; i++) {
    if (sm.find_data(i) != std::to_string(i)) {
      return false;
    }
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

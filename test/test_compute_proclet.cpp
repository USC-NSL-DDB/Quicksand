#include <iostream>
#include <ranges>

#include "nu/compute_proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/vector_task_range.hpp"

bool run() {
  auto cp = nu::ComputeProclet<nu::VectorTaskRange<int>>();
  std::vector<int> inputs{1, 2, 3};
  auto outputs_vectors = cp.run(
      +[](nu::VectorTaskRange<int> &task_range) {
        std::vector<int> outputs;
        while (!task_range.empty()) {
          outputs.emplace_back(task_range.pop());
        }
        return outputs;
      },
      nu::VectorTaskRange<int>(inputs));
  auto join_view = std::ranges::join_view(outputs_vectors);
  return std::vector<int>(join_view.begin(), join_view.end()) == inputs;
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    if (run()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}

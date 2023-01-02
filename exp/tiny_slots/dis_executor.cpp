#include <vector>

#include "runtime.h"

#include "nu/runtime.hpp"
#include "nu/proclet.hpp"
#include "nu/dis_executor.hpp"
#include "nu/vector_task_range.hpp"

void do_work() {
  auto num_tasks = 1 << 10;

  std::vector<std::size_t> inputs;
  for (auto i : std::views::iota(0, num_tasks)) {
    inputs.push_back(i);
  }

  auto dis_exec = nu::make_distributed_executor(
      +[](nu::VectorTaskRange<std::size_t> &task_range) {
        while (1)
          ;
	return true;
      },
      nu::VectorTaskRange<std::size_t>(inputs));
  auto outputs_vector = dis_exec.get();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}

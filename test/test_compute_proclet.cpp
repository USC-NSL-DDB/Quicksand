#include <iostream>
#include <ranges>

#include "nu/compute_proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_ds_range.hpp"
#include "nu/sharded_vector.hpp"
#include "nu/vector_task_range.hpp"
#include "nu/zipped_ds_range.hpp"

bool test_vector_task_range() {
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

bool test_sharded_ds_range() {
  constexpr std::size_t kSize = 50 << 20;
  auto vec = nu::make_sharded_vector<std::size_t, std::false_type>();
  std::size_t sum = 0;
  for (std::size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
    sum += i;
  }
  auto sealed_vec = nu::to_sealed_ds(std::move(vec));
  auto sharded_ds_range = nu::make_sharded_ds_range(sealed_vec);
  auto cp = nu::ComputeProclet<decltype(sharded_ds_range)>();
  auto outputs_vectors = cp.run(
      +[](decltype(sharded_ds_range) &task_range) {
        uint64_t sum = 0;
        while (!task_range.empty()) {
          auto shard_range = task_range.pop();
	  for (const auto &data : shard_range) {
            sum += data;
          }
        }
        return sum;
      },
      sharded_ds_range);
  return std::accumulate(outputs_vectors.begin(), outputs_vectors.end(),
                         static_cast<std::size_t>(0)) == sum;
}

bool test_cont_ds_range() {
  constexpr std::size_t kSize = 50 << 20;
  auto vec = nu::make_sharded_vector<std::size_t, std::false_type>();
  std::size_t sum = 0;
  for (std::size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
    sum += i;
  }
  auto sealed_vec = nu::to_sealed_ds(std::move(vec));
  auto cont_ds_range = nu::make_contiguous_ds_range(sealed_vec);
  auto cp = nu::ComputeProclet<decltype(cont_ds_range)>();
  auto outputs_vectors = cp.run(
      +[](decltype(cont_ds_range) &task_range) {
        uint64_t sum = 0;
        while (!task_range.empty()) {
          sum += task_range.pop();
        }
        return sum;
      },
      cont_ds_range);
  return std::accumulate(outputs_vectors.begin(), outputs_vectors.end(),
                         static_cast<std::size_t>(0)) == sum;
}

bool test_zipped_ds_range() {
  constexpr std::size_t kSize = 50 << 20;
  auto vec0 = nu::make_sharded_vector<std::size_t, std::false_type>();
  auto vec1 = nu::make_sharded_vector<std::size_t, std::false_type>();
  std::size_t sum = 0;
  for (std::size_t i = 0; i < kSize; i++) {
    vec0.push_back(i);
    vec1.push_back(kSize - i);
    sum += kSize;
  }
  auto sealed_vec0 = nu::to_sealed_ds(std::move(vec0));
  auto sealed_vec1 = nu::to_sealed_ds(std::move(vec1));
  auto zipped_ds_range = nu::make_zipped_ds_range(sealed_vec0, sealed_vec1);
  auto cp = nu::ComputeProclet<decltype(zipped_ds_range)>();
  auto outputs_vectors = cp.run(
      +[](decltype(zipped_ds_range) &task_range) {
        uint64_t sum = 0;
        while (!task_range.empty()) {
          auto tuple = task_range.pop();
          sum += std::get<0>(tuple) + std::get<1>(tuple);
        }
        return sum;
      },
      zipped_ds_range);
  return std::accumulate(outputs_vectors.begin(), outputs_vectors.end(),
                         static_cast<std::size_t>(0)) == sum;
}

bool test_all() {
  return test_vector_task_range() && test_sharded_ds_range() &&
         test_cont_ds_range() && test_zipped_ds_range();
}

int main(int argc, char **argv) {
  return nu::runtime_main_init(argc, argv, [](int, char **) {
    if (test_all()) {
      std::cout << "Passed" << std::endl;
    } else {
      std::cout << "Failed" << std::endl;
    }
  });
}

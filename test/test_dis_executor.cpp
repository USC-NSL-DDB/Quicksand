#include <iostream>
#include <iterator>
#include <ranges>

#include "nu/dis_executor.hpp"
#include "nu/runtime.hpp"
#include "nu/sharded_ds_range.hpp"
#include "nu/sharded_vector.hpp"
#include "nu/vector_task_range.hpp"
#include "nu/zipped_ds_range.hpp"

bool test_vector_task_range() {
  constexpr std::size_t kSize = 1 << 20;

  std::vector<std::size_t> inputs;
  for (auto i : std::views::iota(static_cast<std::size_t>(0), kSize)) {
    inputs.push_back(i);
  }

  auto dis_exec = nu::make_distributed_executor(
      +[](nu::VectorTaskRange<std::size_t> &task_range) {
        std::vector<std::size_t> outputs;
        while (true) {
          auto popped = task_range.pop();
          if (unlikely(!popped)) {
            break;
          }
          outputs.emplace_back(*popped);
        }
        return outputs;
      },
      nu::VectorTaskRange<std::size_t>(inputs));
  auto outputs_vectors = dis_exec.get();

  auto join_view = std::ranges::join_view(outputs_vectors);
  return std::vector<std::size_t>(join_view.begin(), join_view.end()) == inputs;
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
  auto dis_exec = nu::make_distributed_executor(
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
  auto outputs_vectors = dis_exec.get();

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
  auto dis_exec = nu::make_distributed_executor(
      +[](decltype(cont_ds_range) &task_range) {
        uint64_t sum = 0;
        while (true) {
          auto popped = task_range.pop();
          if (!popped) {
            break;
          }
          sum += *popped;
        }
        return sum;
      },
      cont_ds_range);
  auto outputs_vectors = dis_exec.get();

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
  auto dis_exec = nu::make_distributed_executor(
      +[](decltype(zipped_ds_range) &task_range) {
        uint64_t sum = 0;
        while (true) {
          auto maybe_tuple = task_range.pop();
          if (unlikely(!maybe_tuple)) {
            break;
          }
          auto tuple = *maybe_tuple;
          sum += std::get<0>(tuple) + std::get<1>(tuple);
        }
        return sum;
      },
      zipped_ds_range);
  auto outputs_vectors = dis_exec.get();

  return std::accumulate(outputs_vectors.begin(), outputs_vectors.end(),
                         static_cast<std::size_t>(0)) == sum;
}

bool test_sharded_vector_filtering() {
  constexpr std::size_t kSize = 50 << 20;
  constexpr static auto kFilterFn = [](std::size_t n) { return n % 2 == 0; };

  auto vec = nu::make_sharded_vector<std::size_t, std::false_type>();
  std::vector<std::size_t> expected;
  for (std::size_t i = 0; i < kSize; i++) {
    vec.push_back(i);
    if (kFilterFn(i)) {
      expected.push_back(i);
    }
  }

  auto sealed_vec = nu::to_sealed_ds(std::move(vec));
  auto cont_vector_range = nu::make_contiguous_ds_range(sealed_vec);
  auto dis_exec = nu::make_distributed_executor(
      +[](decltype(cont_vector_range) &elems) {
        auto filtered_vec =
            nu::make_sharded_vector<std::size_t, std::false_type>();
        std::copy_if(elems.begin(), elems.end(),
                     nu::back_inserter(filtered_vec), kFilterFn);
        filtered_vec.flush();
        return filtered_vec;
      },
      cont_vector_range);
  auto filtered_vecs = dis_exec.get();

  for (auto &vec : filtered_vecs | std::views::drop(1)) {
    filtered_vecs.front().concat(std::move(vec));
  }
  return filtered_vecs.front().collect().unwrap().data() == expected;
}

bool test_all() {
  return test_vector_task_range() && test_sharded_ds_range() &&
         test_cont_ds_range() && test_zipped_ds_range() &&
         test_sharded_vector_filtering();
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

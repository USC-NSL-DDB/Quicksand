/* Copyright (c) 2007-2011, Stanford University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Stanford University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

extern "C" {
#include <runtime/runtime.h>
#include <runtime/timer.h>
}
#include <algorithm>
#include <cereal/types/vector.hpp>
#include <cmath>
#include <cstddef>
#include <fstream>
#include <limits>
#include <memory>
#include <nu/dis_hash_table.hpp>
#include <nu/utils/robin_hood.h>
#include <queue>
#include <span>
#include <sync.h>
#include <thread.h>
#include <unordered_map>
#include <vector>

#include "combiner.h"
#include "stddefines.h"

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner =
              buffer_combiner,
          class Hash = robin_hood::hash<K>>
class MapReduce {
public:
  /* Standard data types for the function arguments and results */
  typedef D data_type;
  typedef V value_type;
  typedef K key_type;
  typedef std::unordered_map<K, Combiner<V, std::allocator>, Hash>
      map_container;
  typedef Combiner<V, std::allocator>::combined reduce_iterator;

  struct keyval {
    key_type key;
    value_type val;

    template <class Archive> void serialize(Archive &ar) { ar(key, val); }
  };

  constexpr static uint32_t kDefaultNumWorkers = 46 * 4;
  constexpr static uint32_t kDefaultChunkSize = 1;
  constexpr static uint32_t kDefaultNumBucketsPerHashTableShard = 64;

  using HashTable = nu::DistributedHashTable<
      K, typename Combiner<V, std::allocator>::combined, Hash, std::equal_to<K>,
      kDefaultNumBucketsPerHashTableShard>;

protected:
  std::vector<std::vector<keyval>> final_vals; // Array to send to merge task.

  // the default split function...
  int split(data_type &a) { return 0; }

  // the default map function...
  void map(data_type &a, map_container &m) const {}

  // the default reduce function...
  void reduce(key_type const &key, reduce_iterator const &values,
              std::vector<keyval> &out) const {
    value_type val;
    while (values.next(val)) {
      keyval kv = {key, val};
      out.push_back(kv);
    }
  }

public:
  MapReduce(uint64_t num_workers = kDefaultNumWorkers)
      : hash_table(new HashTable(nu::bsr_64(num_workers - 1) + 1)) {
    for (uint64_t i = 0; i < num_workers; i++) {
      workers.emplace_back(
          nu::RemObj<MapReduce>::create(hash_table->get_cap()));
    }
  }

  MapReduce(HashTable::Cap cap)
      : hash_table(new HashTable(cap)), map_container_ptr(new map_container()) {
  }

  /* The main MapReduce engine. This is the function called by the
   * application. It is responsible for creating and scheduling all map
   * and reduce tasks, and also organizes and maintains the data which is
   * passed from application to map tasks, map tasks to reduce tasks, and
   * reduce tasks back to the application. Results are stored in result.
   * A return value less than zero represents an error. This function is
   * not thread safe.
   */
  int run(data_type *data, uint64_t count, std::vector<keyval> &result,
          uint64_t chunk_size = kDefaultChunkSize);

  // This version assumes that the split function is provided.
  int run(std::vector<keyval> &result, uint64_t chunk_size = kDefaultChunkSize);

  virtual void run_map(data_type *data, uint64_t len, uint64_t chunk_size);

  virtual void run_reduce();

  virtual void run_merge();

  void emit_intermediate(map_container &i, key_type const &k,
                         value_type const &v) const {
    i[k].add(v);
  }

private:
  std::unique_ptr<HashTable> hash_table;
  std::vector<nu::RemObj<MapReduce>> workers;
  std::unique_ptr<map_container> map_container_ptr;

  void map_chunk(std::vector<data_type> data_chunk);
  void shuffle();
  void __run_map(data_type *data, uint64_t len, uint64_t chunk_size);
  void __run_map_bsp(data_type *data, uint64_t len, uint64_t chunk_size);
};

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
int MapReduce<Impl, D, K, V, Combiner, Hash>::run(std::vector<keyval> &result,
                                                  uint64_t chunk_size) {
  timespec begin;
  std::vector<D> data;
  uint64_t count;
  D chunk;

  // Run splitter to generate chunks
  get_time(begin);
  while (static_cast<Impl *>(this)->split(chunk)) {
    data.push_back(chunk);
  }
  count = data.size();
  print_time_elapsed("split phase", begin);

  return run(&data[0], count, result, chunk_size);
}

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
int MapReduce<Impl, D, K, V, Combiner, Hash>::run(D *data, uint64_t count,
                                                  std::vector<keyval> &result,
                                                  uint64_t chunk_size) {
  auto t0 = microtime();
  run_map(data, count, chunk_size);
  auto t1 = microtime();  
  run_reduce();
  auto t2 = microtime();
  run_merge();
  result.swap(this->final_vals[0]);
  auto t3 = microtime();

  std::cout << t1 - t0 << " " << t2 - t1 << " " << t3 - t2 << std::endl;

  return 0;
}

/**
 * Run map tasks and get intermediate values
 */
template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::run_map(data_type *data,
                                                       uint64_t count,
                                                       uint64_t chunk_size) {
#ifndef BSP
  __run_map(data, count, chunk_size);
#else
  __run_map_bsp(data, count, chunk_size);
#endif
}

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::__run_map(data_type *data,
                                                         uint64_t count,
                                                         uint64_t chunk_size) {
  std::vector<nu::Future<void>> futures;
  futures.resize(workers.size());

  uint32_t dispatch_id = 0;
  std::vector<data_type> data_chunk;

  for (uint64_t start_id = 0; start_id < count; start_id += chunk_size) {
    auto chunk_idx_begin = start_id;
    auto chunk_idx_end = std::min(chunk_size + start_id, count);

    data_chunk.clear();
    for (uint32_t i = chunk_idx_begin; i < chunk_idx_end; i++) {
      data_chunk.push_back(data[i]);
    }

    while (futures[dispatch_id] && !futures[dispatch_id].is_ready()) {
      dispatch_id++;
      if (unlikely(dispatch_id == workers.size())) {
        dispatch_id = 0;
      }
    }

    futures[dispatch_id] = workers[dispatch_id].run_async(
        &MapReduce::map_chunk, data_chunk);
  }
}

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::__run_map_bsp(
    data_type *data, uint64_t count, uint64_t chunk_size) {
  std::vector<nu::Future<void>> futures;
  futures.resize(workers.size());

#ifdef BSP_PRINT_STAT
  std::ofstream bsp_stat_ofs("bsp_stat");
  std::vector<uint64_t> timestamps;
  timestamps.reserve((count - 1) / workers.size() + 1);
#endif

  for (uint64_t start_id = 0; start_id < count; start_id += workers.size()) {
#ifdef BSP_PRINT_STAT
    timestamps.push_back(microtime());
#endif
    auto batch_size = std::min(workers.size(), count - start_id);
    for (uint32_t i = 0; i < batch_size; i++) {
      std::vector<data_type> data_chunk(1, data[start_id + i]);
      futures[i] = workers[i].run_async(&MapReduce::map_chunk, data_chunk);
    }

    for (auto &future : futures) {
      future.get();
    }
  }

#ifdef BSP_PRINT_STAT
  for (auto timestamp : timestamps) {
    bsp_stat_ofs << timestamp << std::endl;
  }
#endif
  }

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::shuffle() {
  std::vector<nu::Future<void>> futures;
  for (auto &[k, combiner] : *map_container_ptr) {
    futures.emplace_back(hash_table->apply_async(
        k,
        +[](std::pair<const K, typename Combiner<V, std::allocator>::combined>
                &p,
            Combiner<V, std::allocator> &&combiner) {
          combiner.combineinto(p.second);
        },
        std::move(combiner)));
  }
}

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::map_chunk(
    std::vector<data_type> data_chunk) {
  for (auto &data : data_chunk) {
    static_cast<Impl const *>(this)->map(data, *map_container_ptr);
  }
}

/**
 * Run reduce tasks and get final values.
 */
template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::run_reduce() {
  std::vector<nu::Future<void>> futures;
  futures.reserve(workers.size());
  for (auto &worker : workers) {
    futures.emplace_back(worker.run_async(&MapReduce::shuffle));
  }
  for (auto &future : futures) {
    future.get();
  }

  final_vals = hash_table->associative_reduce(
      std::vector<keyval>(),
      +[](std::vector<keyval> &pairs,
          std::pair<const K, typename Combiner<V, std::allocator>::combined>
              &p) {
        Impl *stateless = nullptr;
        stateless->reduce(p.first, p.second, pairs);
      });
}

/**
 * Merge all reduced data
 */
template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::run_merge() {
  uint32_t indices[final_vals.size() + 1];
  indices[0] = 0;

  for (uint32_t i = 0; i < final_vals.size(); i++) {
    auto &partition = final_vals[i];
    indices[i + 1] = indices[i] + partition.size();
  }
  std::vector<std::vector<keyval>> final(1);
  final[0].resize(indices[final_vals.size()]);

  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < final_vals.size(); i++) {
    threads.emplace_back([&, tid = i] {
      auto &partition = final_vals[tid];
      for (uint32_t j = indices[tid]; j < indices[tid + 1]; j++) {
        final[0][j] = partition[j - indices[tid]];
      }
    });
  }
  for (auto &thread : threads) {
    thread.Join();
  }

  this->final_vals = std::move(final);
}

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner =
              buffer_combiner,
          class Hash = std::hash<K>>
class MapReduceSort : public MapReduce<Impl, D, K, V, Combiner, Hash> {
public:
  typedef typename MapReduce<Impl, D, K, V, Combiner, Hash>::keyval keyval;

protected:
  // default sorting order is by key. User can override.
  bool sort(keyval const &a, keyval const &b) const { return a.key < b.key; }

  struct sort_functor {
    MapReduceSort *mrs;
    sort_functor(MapReduceSort *mrs) : mrs(mrs) {}
    bool operator()(keyval const &a, keyval const &b) const {
      return static_cast<Impl const *>(mrs)->sort(a, b);
    }
  };

  virtual void run_merge() {
    // how many lists to merge in a single task.
    static const int merge_factor = 2;
    int merge_queues = this->final_vals.size();

    std::vector<rt::Thread> threads;
    // First sort each queue in place
    for (int i = 0; i < merge_queues; i++) {
      threads.emplace_back(
          [&, i, val = &this->final_vals[i]] { merge_fn(val, 0, i); });
    }

    for (auto &thread : threads) {
      thread.Join();
    }
    threads.clear();

    // Then merge
    std::vector<std::vector<keyval>> merge_vals;
    while (merge_queues > 1) {
      uint64_t resulting_queues =
          (uint64_t)std::ceil(merge_queues / (double)merge_factor);

      // swap queues
      merge_vals = std::move(this->final_vals);
      this->final_vals.clear();
      this->final_vals.resize(resulting_queues);

      // distribute tasks into task queues using locality information
      // if provided.
      int queue_index = 0;
      for (uint64_t i = 0; i < resulting_queues; i++) {
        int actual = std::min(merge_factor, merge_queues - queue_index);
        threads.emplace_back([&, val = &merge_vals[queue_index], actual, i] {
          merge_fn(val, actual, i);
        });
        queue_index += actual;
      }

      for (auto &thread : threads) {
        thread.Join();
      }
      threads.clear();

      merge_vals.clear();
      merge_queues = resulting_queues;
    }

    assert(merge_queues == 1);
  }

  void merge_fn(std::vector<keyval> *vals, uint64_t length,
                uint64_t out_index) {
    if (length == 0) {
      // this case really just means sort my list in place.
      // stable_sort ensures that the order of same keyvals with
      // the same key emitted in reduce remains the same in sort
      std::stable_sort(vals->begin(), vals->end(), sort_functor(this));
    } else if (length == 1) {
      // if only one list, don't merge, just move over.
      (*vals).swap(this->final_vals[out_index]);
    } else if (length == 2) {
      // stl merge is nice and fast for 2.
      this->final_vals[out_index].resize(vals[0].size() + vals[1].size());
      std::merge(vals[0].begin(), vals[0].end(), vals[1].begin(), vals[1].end(),
                 this->final_vals[out_index].begin(), sort_functor(this));
    } else {
      // for more, do a multiway merge.
      assert(0);
    }
  }
};

#endif // MAP_REDUCE_H_

// vim: ts=8 sw=4 sts=4 smarttab smartindent

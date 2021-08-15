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
}
#include <algorithm>
#include <cmath>
#include <limits>
#include <nu/dis_hash_table.hpp>
#include <queue>
#include <thread.h>
#include <vector>

#include "combiner.h"
#include "container.h"
#include "stddefines.h"

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner =
              buffer_combiner,
          class Hash = std::hash<K>>
class MapReduce {
public:
  /* Standard data types for the function arguments and results */
  typedef D data_type;
  typedef V value_type;
  typedef K key_type;
  typedef hash_container<K, V, Combiner, Hash> container_type;

  typedef typename container_type::input_type map_container;
  typedef typename container_type::output_type reduce_iterator;

  struct keyval {
    key_type key;
    value_type val;
  };

protected:
  // Parameters.
  uint64_t num_threads; // # of threads to run.

  container_type container;
  std::vector<keyval> *final_vals; // Array to send to merge task.

  uint64_t num_map_tasks;
  uint64_t num_reduce_tasks;

  std::vector<rt::Thread> tasks;

  virtual void run_map(data_type *data, uint64_t len);
  virtual void run_reduce();
  virtual void run_merge();

  // the default split function...
  int split(data_type &a) { return 0; }

  // the default map function...
  void map(data_type const &a, map_container &m) const {}

  // the default reduce function...
  void reduce(key_type const &key, reduce_iterator const &values,
              std::vector<keyval> &out) const {
    value_type val;
    while (values.next(val)) {
      keyval kv = {key, val};
      out.push_back(kv);
    }
  }

  // the default locator function...
  void *locate(data_type *data, uint64_t) const { return (void *)data; }

public:
  MapReduce() { num_threads = maxks; }

  virtual ~MapReduce() {}

  /* The main MapReduce engine. This is the function called by the
   * application. It is responsible for creating and scheduling all map
   * and reduce tasks, and also organizes and maintains the data which is
   * passed from application to map tasks, map tasks to reduce tasks, and
   * reduce tasks back to the application. Results are stored in result.
   * A return value less than zero represents an error. This function is
   * not thread safe.
   */
  int run(data_type *data, uint64_t count, std::vector<keyval> &result);

  // This version assumes that the split function is provided.
  int run(std::vector<keyval> &result);

  void emit_intermediate(typename container_type::input_type &i,
                         key_type const &k, value_type const &v) const {
    i[k].add(v);
  }
};

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
int MapReduce<Impl, D, K, V, Combiner, Hash>::run(std::vector<keyval> &result) {
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

  return run(&data[0], count, result);
}

template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
int MapReduce<Impl, D, K, V, Combiner, Hash>::run(D *data, uint64_t count,
                                                  std::vector<keyval> &result) {
  timespec begin;
  timespec run_begin = get_time();
  // Initialize library
  get_time(begin);

  // Compute task counts (should make this more adjustable) and then
  // allocate storage
  this->num_map_tasks = std::min(count, this->num_threads) * 16;
  this->num_reduce_tasks = this->num_threads;
  dprintf("num_map_tasks = %d\n", num_map_tasks);
  dprintf("num_reduce_tasks = %d\n", num_reduce_tasks);

  container.init(this->num_threads, this->num_reduce_tasks);
  this->final_vals = new std::vector<keyval>[this->num_threads];
  for (uint64_t i = 0; i < this->num_threads; i++) {
    // Try to avoid a reallocation. Very costly on Solaris.
    this->final_vals[i].reserve(100);
  }
  print_time_elapsed("library init", begin);

  // Run map tasks and get intermediate values
  get_time(begin);
  run_map(&data[0], count);
  print_time_elapsed("map phase", begin);

  dprintf(
      "In scheduler, all map tasks are done, now scheduling reduce tasks\n");

  // Run reduce tasks and get final values
  get_time(begin);
  run_reduce();
  print_time_elapsed("reduce phase", begin);

  dprintf(
      "In scheduler, all reduce tasks are done, now scheduling merge tasks\n");

  get_time(begin);
  run_merge();
  print_time_elapsed("merge phase", begin);

  result.swap(*this->final_vals);

  // Delete structures
  delete[] this->final_vals;

  print_time_elapsed("run time", run_begin);

  return 0;
}

/**
 * Run map tasks and get intermediate values
 */
template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::run_map(data_type *data,
                                                       uint64_t count) {
  std::vector<rt::Thread> threads;

  // Compute map task chunk size
  uint64_t chunk_size =
      std::max(1, (int)ceil((double)count / this->num_map_tasks));

  // Generate tasks by splitting input data and add to queue.
  for (uint64_t i = 0; i < this->num_map_tasks; i++) {
    uint64_t start = chunk_size * i;

    if (start < count) {
      data_type *data_start = data + start;
      uint64_t len = std::min(chunk_size, count - start);
      threads.emplace_back([&, data_start, len] {
        auto core_id = get_cpu();
        typename container_type::input_type t = container.get(core_id);
        for (data_type *data = data_start; data < data_start + len; ++data) {
          static_cast<Impl const *>(this)->map(*data, t);
        }
        container.add(core_id, t);
        put_cpu();
      });
    }
  }

  for (auto &thread : threads) {
    thread.Join();
  }
}

/**
 * Run reduce tasks and get final values.
 */
template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::run_reduce() {
  std::vector<rt::Thread> threads;

  // Create tasks and enqueue...
  for (uint64_t i = 0; i < this->num_reduce_tasks; ++i) {
    threads.emplace_back([&, reduce_id = i] {
      typename container_type::iterator iter = container.begin(reduce_id);
      K key;
      reduce_iterator values;
      while (iter.next(key, values)) {
        if (values.size() > 0)
          static_cast<Impl const *>(this)->reduce(key, values,
                                                  this->final_vals[reduce_id]);
      }
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }
}

/**
 * Merge all reduced data
 */
template <typename Impl, typename D, typename K, typename V,
          template <typename, template <class> class> class Combiner,
          class Hash>
void MapReduce<Impl, D, K, V, Combiner, Hash>::run_merge() {
  size_t total = 0;
  for (size_t i = 0; i < num_threads; i++) {
    total += this->final_vals[i].size();
  }

  std::vector<keyval> *final = new std::vector<keyval>[1];
  final[0].reserve(total);

  for (size_t i = 0; i < num_threads; i++) {
    final[0].insert(final[0].end(), this->final_vals[i].begin(),
                    this->final_vals[i].end());
  }

  delete[] this->final_vals;
  this->final_vals = final;
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
    int merge_queues = this->num_threads;

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
    std::vector<keyval> *merge_vals;
    while (merge_queues > 1) {
      uint64_t resulting_queues =
          (uint64_t)std::ceil(merge_queues / (double)merge_factor);

      // swap queues
      merge_vals = this->final_vals;
      this->final_vals = new std::vector<keyval>[resulting_queues];

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

      delete[] merge_vals;
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

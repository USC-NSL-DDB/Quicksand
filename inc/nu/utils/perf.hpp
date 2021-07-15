#pragma once

#include <cstdint>
#include <memory>
#include <thread.h>
#include <utility>
#include <vector>

#include "nu/commons.hpp"

namespace nu {

class PerfRequest {};

struct PerfRequestWithTime {
  uint64_t start_us;
  std::unique_ptr<PerfRequest> req;
};

struct Trace {
  uint64_t start_us;
  uint64_t duration_us;
};

struct PerfThreadState {
  virtual ~PerfThreadState() = default;
};

class PerfAdapter {
public:
  virtual std::unique_ptr<PerfThreadState> create_thread_state() = 0;
  virtual std::unique_ptr<PerfRequest> gen_req(PerfThreadState *state) = 0;
  virtual void serve_req(PerfThreadState *state, const PerfRequest *perf) = 0;
};

// Open-loop, possion arrival.
class Perf {
public:
  Perf(PerfAdapter &adapter);
  void reset();
  void run(uint32_t num_threads, double target_mops, uint64_t duration_us,
           // To filter out abnormally long durations caused by OS-level
           // interruptions.
           uint64_t max_req_us = 5 * kOneMilliSecond);
  uint64_t get_overall_lat(double nth);
  std::vector<std::pair<uint64_t, uint64_t>>
  get_timeseries_lats(uint64_t interval_us, double nth);
  double get_real_mops() const;

private:
  enum TraceFormat { UNSORTED, SORTED_BY_DURATION, SORTED_BY_START };

  PerfAdapter &adapter_;
  std::vector<Trace> traces_;
  TraceFormat trace_format_;
  double real_mops_;
  friend class Test;

  void gen_reqs(std::vector<PerfRequestWithTime> *all_reqs,
                uint32_t num_threads, double target_mops, uint64_t duration_us);
  std::vector<Trace> benchmark(std::vector<PerfRequestWithTime> *all_reqs,
                               uint32_t num_threads, uint64_t max_req_us);
};

} // namespace nu

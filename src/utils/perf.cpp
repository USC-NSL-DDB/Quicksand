#include <algorithm>
#include <cmath>
#include <random>
extern "C" {
#include <runtime/timer.h>
}

#include "nu/utils/perf.hpp"

namespace nu {

Perf::Perf(PerfAdapter &adapter)
    : adapter_(adapter), trace_format_(UNSORTED), real_mops_(0) {}

void Perf::reset() {
  traces_.clear();
  trace_format_ = UNSORTED;
  real_mops_ = 0;
}

void Perf::gen_reqs(
    std::vector<PerfRequestWithTime> *all_reqs,
    const std::vector<std::unique_ptr<PerfThreadState>> &thread_states,
    uint32_t num_threads, double target_mops, uint64_t duration_us) {
  std::vector<rt::Thread> threads;

  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(
        [&, &reqs = all_reqs[i], thread_state = thread_states[i].get()] {
          std::random_device rd;
          std::mt19937 gen(rd());
          std::exponential_distribution<double> d(target_mops / num_threads);
          uint64_t cur_us = 0;

          while (cur_us < duration_us) {
            auto interval = std::max(1l, std::lround(d(gen)));
            PerfRequestWithTime req_with_time;
            req_with_time.start_us = cur_us;
            req_with_time.req = adapter_.gen_req(thread_state);
            reqs.emplace_back(std::move(req_with_time));
            cur_us += interval;
          }
        });
  }

  for (auto &thread : threads) {
    thread.Join();
  }
}

std::vector<Trace> Perf::benchmark(
    std::vector<PerfRequestWithTime> *all_reqs,
    const std::vector<std::unique_ptr<PerfThreadState>> &thread_states,
    uint32_t num_threads, uint64_t max_req_us) {
  std::vector<rt::Thread> threads;
  std::vector<Trace> all_traces[num_threads];

  for (uint32_t i = 0; i < num_threads; i++) {
    all_traces[i].reserve(all_reqs[i].size());
  }

  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back([&, &reqs = all_reqs[i], &traces = all_traces[i],
                          thread_state = thread_states[i].get()] {
      auto start_us = microtime();
      bool skipping = false;

      for (const auto &req : reqs) {
        auto relative_us = microtime() - start_us;
        if (req.start_us > relative_us) {
          timer_sleep(req.start_us - relative_us);
        } else if (skipping) {
          continue;
        }
        skipping = false;
        bool ok = adapter_.serve_req(thread_state, req.req.get());
        Trace trace;
        trace.start_us = req.start_us;
        trace.duration_us = microtime() - start_us - trace.start_us;
        if (!ok || trace.duration_us > max_req_us) {
          // Timeouted or an OS-level interruption occured. In this case, we
          // skip all missed reqs.
          skipping = true;
        } else {
          traces.push_back(trace);
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }

  std::vector<Trace> gathered_traces;
  for (uint32_t i = 0; i < num_threads; i++) {
    gathered_traces.insert(gathered_traces.end(), all_traces[i].begin(),
                           all_traces[i].end());
  }
  return gathered_traces;
}

void Perf::create_thread_states(
    std::vector<std::unique_ptr<PerfThreadState>> *thread_states,
    uint32_t num_threads) {
  for (uint32_t i = 0; i < num_threads; i++) {
    thread_states->emplace_back(adapter_.create_thread_state());
  }
}

void Perf::run(uint32_t num_threads, double target_mops, uint64_t duration_us,
               uint64_t warmup_us, uint64_t max_req_us) {
  std::vector<std::unique_ptr<PerfThreadState>> thread_states;
  create_thread_states(&thread_states, num_threads);
  std::vector<PerfRequestWithTime> all_warmup_reqs[num_threads];
  std::vector<PerfRequestWithTime> all_perf_reqs[num_threads];
  gen_reqs(all_warmup_reqs, thread_states, num_threads, target_mops, warmup_us);
  gen_reqs(all_perf_reqs, thread_states, num_threads, target_mops, duration_us);
  benchmark(all_warmup_reqs, thread_states, num_threads, max_req_us);
  traces_ =
      move(benchmark(all_perf_reqs, thread_states, num_threads, max_req_us));
  real_mops_ = static_cast<double>(traces_.size()) / duration_us;
}

uint64_t Perf::get_average_lat() {
  if (trace_format_ != SORTED_BY_DURATION) {
    std::sort(traces_.begin(), traces_.end(),
              [](const Trace &x, const Trace &y) {
                return x.duration_us < y.duration_us;
              });
    trace_format_ = SORTED_BY_DURATION;
  }

  auto sum = std::accumulate(
      std::next(traces_.begin()), traces_.end(), 0ULL,
      [](uint64_t sum, const Trace &t) { return sum + t.duration_us; });
  return sum / traces_.size();
}

uint64_t Perf::get_nth_lat(double nth) {
  if (trace_format_ != SORTED_BY_DURATION) {
    std::sort(traces_.begin(), traces_.end(),
              [](const Trace &x, const Trace &y) {
                return x.duration_us < y.duration_us;
              });
    trace_format_ = SORTED_BY_DURATION;
  }

  size_t idx = nth / 100.0 * traces_.size();
  return traces_[idx].duration_us;
}

std::vector<std::pair<uint64_t, uint64_t>>
Perf::get_timeseries_nth_lats(uint64_t interval_us, double nth) {
  std::vector<std::pair<uint64_t, uint64_t>> timeseries;
  if (trace_format_ != SORTED_BY_START) {
    std::sort(
        traces_.begin(), traces_.end(),
        [](const Trace &x, const Trace &y) { return x.start_us < y.start_us; });
    trace_format_ = SORTED_BY_START;
  }

  auto cur_win_us = traces_.front().start_us;
  std::vector<uint64_t> win_durations;
  for (auto &trace : traces_) {
    if (cur_win_us + interval_us < trace.start_us) {
      std::sort(win_durations.begin(), win_durations.end());
      size_t idx = nth / 100.0 * win_durations.size();
      timeseries.emplace_back(cur_win_us, win_durations[idx]);
      cur_win_us += interval_us;
      win_durations.clear();
    }
    win_durations.push_back(trace.duration_us);
  }

  return timeseries;
}

double Perf::get_real_mops() const { return real_mops_; }

} // namespace nu

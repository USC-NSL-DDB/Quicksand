namespace nu {

inline void CPULoad::reset() {
  last_refresh_tsc = 0;
  memset(infos, 0, sizeof(infos));
}

inline CPULoad::State CPULoad::monitor_start() {
  if (likely(infos[read_cpu()].sample_cnt++ % kSampleInterval)) {
    State state;
    state.sampled = false;
    return state;
  }
  return __monitor_start();
}

inline void CPULoad::monitor_end(const State &state) {
  if (likely(!state.sampled)) {
    return;
  }
  __monitor_end(state);
}

} // namespace nu

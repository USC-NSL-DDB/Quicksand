namespace nu {

SplitMix64::SplitMix64(uint64_t x) : x_(x) {}

uint64_t SplitMix64::next() {
  uint64_t z = (x_ += UINT64_C(0x9E3779B97F4A7C15));
  z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
  z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
  return z ^ (z >> 31);
}

}  // namespace nu

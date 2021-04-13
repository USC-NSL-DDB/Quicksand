#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>

extern "C" {
#include <runtime/runtime.h>
}

#include "utils/slab.hpp"

using namespace nu;

constexpr static uint64_t kBufSize = (4ULL << 30);
constexpr static uint64_t kMinSlabClassSize =
    (1ULL << SlabAllocator::kMinSlabClassShift);
constexpr static uint64_t kMaxSlabClassSize = (1ULL << 27);

static_assert(kBufSize >= kMaxSlabClassSize);

bool run_with_size(uint64_t obj_size, uint64_t class_size) {
  auto *buf = new uint8_t[kBufSize];
  std::unique_ptr<uint8_t[]> buf_gc(buf);

  auto slab = std::make_unique<SlabAllocator>(0, buf, kBufSize);
  uint64_t count = kBufSize / class_size;
  if (slab->get_base() != buf) {
    return false;
  }

  for (uint64_t i = 0; i < count; i++) {
    if (slab->allocate(obj_size) != buf + i * class_size + sizeof(PtrHeader)) {
      return false;
    }
  }
  if (slab->allocate(obj_size) != nullptr) {
    return false;
  }

  slab->free(buf + sizeof(PtrHeader));
  slab->free(buf + class_size + sizeof(PtrHeader));
  if (slab->allocate(obj_size) != buf + class_size + sizeof(PtrHeader)) {
    return false;
  }
  if (slab->allocate(obj_size) != buf + sizeof(PtrHeader)) {
    return false;
  }

  return true;
}

bool run_min_size() { return run_with_size(1, kMinSlabClassSize); }

bool run_mid_size() {
  return run_with_size(110, 128) & run_with_size(200, 256);
}

bool run_max_size() {
  return run_with_size(kMaxSlabClassSize - sizeof(PtrHeader),
                       kMaxSlabClassSize);
}

bool run_more_than_buf_size() {
  auto *buf = new uint8_t[kBufSize];
  std::unique_ptr<uint8_t[]> buf_gc(buf);

  auto slab = std::make_unique<SlabAllocator>(0, buf, kBufSize);
  if (slab->allocate(kBufSize - sizeof(PtrHeader) + 1) != nullptr) {
    return false;
  }
  return true;
}

bool run() {
  return run_min_size() & run_mid_size() & run_max_size() &
         run_more_than_buf_size();
}

void _main(void *args) {
  std::cout << "Running " << __FILE__ "..." << std::endl;
  if (run()) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = runtime_init(argv[1], _main, NULL);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

#include <asm/mman.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <sys/mman.h>

extern "C" {
#include <base/assert.h>
#include <runtime/thread.h>
}

#include "nu/proclet_mgr.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

namespace nu {

ProcletManager::ProcletManager() {
  num_present_proclets_ = 0;
  for (uint64_t vaddr = kMinProcletHeapVAddr;
       vaddr + kProcletHeapSize <= kMaxProcletHeapVAddr;
       vaddr += kProcletHeapSize) {
    auto *proclet_base = reinterpret_cast<ProcletHeader *>(vaddr);
    auto mmap_addr =
        ::mmap(proclet_base, kNumAlwaysMmapedBytes, PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, -1, 0);
    auto *proclet_header = reinterpret_cast<ProcletHeader *>(mmap_addr);
    proclet_header->status = kAbsent;
    std::construct_at(&proclet_header->rcu_lock);
    std::construct_at(&proclet_header->spin_lock);
    std::construct_at(&proclet_header->cond_var);
  }
}

void ProcletManager::allocate(void *proclet_base, bool migratable) {
  auto mmap_addr =
      ::mmap(reinterpret_cast<uint8_t *>(proclet_base) + kNumAlwaysMmapedBytes,
             kProcletHeapSize - kNumAlwaysMmapedBytes, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr == reinterpret_cast<void *>(-1));
  setup(proclet_base, migratable, /* from_migration = */ false);
}

void ProcletManager::mmap(void *proclet_base) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  if (proclet_header->status >= kMapped) {
    return;
  }

  proclet_header->spin_lock.lock();
  if (unlikely(proclet_header->status >= kMapped)) {
    proclet_header->spin_lock.unlock();
    return;
  }
  auto *mmap_base =
      reinterpret_cast<uint8_t *>(proclet_base) + kNumAlwaysMmapedBytes;
  auto *mmap_addr = ::mmap(mmap_base, kProcletHeapSize - kNumAlwaysMmapedBytes,
                           PROT_READ | PROT_WRITE,
                           MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != mmap_base);
  barrier();
  proclet_header->status = kMapped;
  proclet_header->spin_lock.unlock();
}

void ProcletManager::madvise_populate(void *proclet_base,
                                      uint64_t populate_len) {
  auto *mmap_base =
      reinterpret_cast<uint8_t *>(proclet_base) + kNumAlwaysMmapedBytes;
  populate_len -= kNumAlwaysMmapedBytes;
  populate_len = ((populate_len - 1) / kPageSize + 1) * kPageSize;
  BUG_ON(madvise(mmap_base, populate_len, MADV_POPULATE_WRITE) != 0);
}

void ProcletManager::deallocate(void *proclet_base) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  proclet_header->spin_lock.lock(); // Sync with PressureHandler.

  proclet_header->status = kAbsent;
  auto *munmap_base =
      reinterpret_cast<uint8_t *>(proclet_base) + kNumAlwaysMmapedBytes;
  auto total_munmap_size = kProcletHeapSize - kNumAlwaysMmapedBytes;
  RuntimeSlabGuard guard;
  std::destroy_at(&proclet_header->blocked_syncer);
  std::destroy_at(&proclet_header->time);
  // FIXME
  // std::destroy_at(&proclet_header->migrated_wg);
  std::destroy_at(&proclet_header->spin);
  std::destroy_at(&proclet_header->slab);
  BUG_ON(munmap(munmap_base, total_munmap_size) == -1);
  proclet_header->spin_lock.unlock();
}

void ProcletManager::setup(void *proclet_base, bool migratable,
                           bool from_migration) {
  RuntimeSlabGuard guard;
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);

  std::construct_at(&proclet_header->blocked_syncer);
  std::construct_at(&proclet_header->time);
  proclet_header->migratable = migratable;
  // FIXME
  // std::construct_at(&proclet_header->migrated_wg);

  if (!from_migration) {
    std::construct_at(&proclet_header->spin);
    proclet_header->ref_cnt = 1;
    auto proclet_region_size = kProcletHeapSize - sizeof(ProcletHeader);
    proclet_header->slab.init(to_slab_id(proclet_header), proclet_header + 1,
                              proclet_region_size);
  }
}

std::vector<void *> ProcletManager::get_all_proclets() {
  {
    rt::SpinGuard guard(&spin_);
    auto iter = present_proclets_.begin();
    for (auto *proclet_base : present_proclets_) {
      auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
      if (proclet_header->status == kPresent) {
        *iter = proclet_base;
        iter++;
      }
    }
    present_proclets_.erase(iter, present_proclets_.end());
  }
  return present_proclets_;
}

uint64_t ProcletManager::get_mem_usage() {
  uint64_t total_mem_usage = 0;
  auto proclets = get_all_proclets();
  for (auto *proclet_base : proclets) {
    auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
    auto &proclet_slab = proclet_header->slab;
    total_mem_usage += reinterpret_cast<uint8_t *>(proclet_slab.get_base()) -
                       reinterpret_cast<uint8_t *>(proclet_header) +
                       proclet_slab.get_usage();
  }

  return total_mem_usage;
}

} // namespace nu

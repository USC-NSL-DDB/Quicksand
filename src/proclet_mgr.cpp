#include <asm/mman.h>
#include <sys/mman.h>

#include <cstdint>
#include <functional>
#include <memory>

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
    auto *proclet_base = reinterpret_cast<uint8_t *>(vaddr);
    auto mmap_addr =
        mmap(proclet_base, kNumAlwaysPopulatedBytes, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, -1, 0);
    BUG_ON(mmap_addr != proclet_base);
    auto *proclet_header = reinterpret_cast<ProcletHeader *>(mmap_addr);
    proclet_header->status = kAbsent;
    std::construct_at(&proclet_header->rcu_lock);
    std::construct_at(&proclet_header->spin_lock);
    std::construct_at(&proclet_header->cond_var);

    auto *populate_addr = proclet_base + kNumAlwaysPopulatedBytes;
    mmap_addr = mmap(populate_addr, kProcletHeapSize - kNumAlwaysPopulatedBytes,
                     PROT_READ | PROT_WRITE,
                     MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
    BUG_ON(mmap_addr != populate_addr);
  }
}

void ProcletManager::madvise_populate(void *proclet_base,
                                      uint64_t populate_len) {
  auto *mmap_base =
      reinterpret_cast<uint8_t *>(proclet_base) + kNumAlwaysPopulatedBytes;
  populate_len -= kNumAlwaysPopulatedBytes;
  populate_len = ((populate_len - 1) / kPageSize + 1) * kPageSize;
  BUG_ON(madvise(mmap_base, populate_len, MADV_POPULATE_WRITE) != 0);
}

void ProcletManager::cleanup(void *proclet_base) {
  auto *proclet_header = reinterpret_cast<ProcletHeader *>(proclet_base);
  proclet_header->spin_lock.lock();  // Sync with PressureHandler.

  proclet_header->status = kAbsent;
  RuntimeSlabGuard guard;
  std::destroy_at(&proclet_header->blocked_syncer);
  std::destroy_at(&proclet_header->time);
  // FIXME
  // std::destroy_at(&proclet_header->migrated_wg);
  std::destroy_at(&proclet_header->spin);
  std::destroy_at(&proclet_header->slab);
  proclet_header->spin_lock.unlock();

  // Use munmap to release physical pages and then use mmap to recreate vmas.
  // This is way faster than the madvise(MADV_FREE) interface.
  auto *munmap_base =
      reinterpret_cast<uint8_t *>(proclet_base) + kNumAlwaysPopulatedBytes;
  auto size = kProcletHeapSize - kNumAlwaysPopulatedBytes;
  BUG_ON(munmap(munmap_base, size) == -1);
  auto mmap_addr = mmap(munmap_base, size, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  BUG_ON(mmap_addr != munmap_base);
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
    auto slab_region_size = kProcletHeapSize - sizeof(ProcletHeader);
    std::construct_at(&proclet_header->slab, to_slab_id(proclet_header),
                      proclet_header + 1, slab_region_size);
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

}  // namespace nu

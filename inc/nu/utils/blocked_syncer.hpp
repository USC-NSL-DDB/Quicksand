#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "nu/utils/farmhash.hpp"
#include "nu/utils/sync_hash_map.hpp"

namespace nu {

class BlockedSyncer {
public:
  enum Type { kMutex = 0, kCondVar };

  void add(void *syncer, Type type);
  void remove(void *syncer);
  std::vector<std::pair<void *, Type>> get_all();

private:
  constexpr static uint32_t kNumBuckets = kNumCores;
  using Key = void *;
  using Val = Type;
  constexpr static auto HashFn = [](void *p) {
    return util::Fingerprint(reinterpret_cast<uint64_t>(p));
  };
  using Hash = decltype(HashFn);
  using KeyEqual = std::equal_to<Key>;
  using Allocator = std::allocator<std::pair<const Key, Val>>;

  SyncHashMap<kNumBuckets, Key, Val, Hash, KeyEqual, Allocator> sync_map_;
};

} // namespace nu

#include "nu/impl/blocked_syncer.ipp"

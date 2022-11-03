#pragma once

#include <map>
#include <optional>
#include <stack>
#include <vector>

#include "nu/shard.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/read_skewed_lock.hpp"

namespace nu {

template <class Shard>
class GeneralShardingMapping {
 public:
  using Key = Shard::Key;

  GeneralShardingMapping(uint32_t max_shard_bytes);
  ~GeneralShardingMapping();
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_shards_in_range(std::optional<Key> l_key, std::optional<Key> r_key);
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_all_shards();
  std::optional<WeakProclet<Shard>> get_shard_for_key(std::optional<Key> key);
  void reserve_new_shard();
  WeakProclet<Shard> create_new_shard(std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      bool reserve_space);
  void inc_ref_cnt();
  void dec_ref_cnt();
  void seal();
  void unseal();

 private:
  constexpr static double kProcletOverprovisionFactor = 3;

  WeakProclet<GeneralShardingMapping> self_;
  uint32_t max_shard_bytes_;
  uint32_t proclet_capacity_;
  std::multimap<std::optional<Key>, Proclet<Shard>> mapping_;
  ReadSkewedLock rw_lock_;
  uint32_t ref_cnt_;
  CondVar ref_cnt_cv_;
  std::stack<Proclet<Shard>> reserved_shards_;
  Mutex mutex_;
};

}  // namespace nu

#include "nu/impl/sharding_mapping.ipp"

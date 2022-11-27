#pragma once

#include <map>
#include <optional>
#include <stack>
#include <vector>

#include "nu/shard.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <class Shard>
class GeneralShardMapping {
 public:
  using Key = Shard::Key;

  GeneralShardMapping(uint32_t max_shard_bytes,
                      std::optional<uint32_t> max_shard_count);
  ~GeneralShardMapping();
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_shards_in_range(std::optional<Key> l_key, std::optional<Key> r_key);
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_all_keys_and_shards();
  std::optional<WeakProclet<Shard>> get_shard_for_key(std::optional<Key> key);
  std::vector<Proclet<Shard>> acquire_all_shards();
  void reserve_new_shard();
  std::optional<WeakProclet<Shard>> create_new_shard(std::optional<Key> l_key,
                                                     std::optional<Key> r_key,
                                                     bool reserve_space);
  bool delete_front_shard();
  void concat(WeakProclet<GeneralShardMapping> tail) requires(
      Shard::GeneralContainer::kContiguousIterator);
  void inc_ref_cnt();
  void dec_ref_cnt();
  void seal();
  void unseal();

 private:
  constexpr static double kProcletOverprovisionFactor = 3;

  WeakProclet<GeneralShardMapping> self_;
  uint32_t max_shard_bytes_;
  uint32_t proclet_capacity_;
  std::optional<uint32_t> max_shard_count_;

  std::multimap<std::optional<Key>, Proclet<Shard>> mapping_;
  uint32_t ref_cnt_;
  CondVar ref_cnt_cv_;
  std::stack<Proclet<Shard>> reserved_shards_;
  Mutex mutex_;

  bool reached_size_bound();
};

}  // namespace nu

#include "nu/impl/shard_mapping.ipp"

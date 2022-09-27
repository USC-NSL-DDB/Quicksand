#pragma once

#include <map>
#include <optional>
#include <stack>
#include <vector>

#include "nu/shard.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/reader_writer_lock.hpp"

namespace nu {

template <class Shard>
class GeneralShardingMapping {
 public:
  using Key = Shard::Key;

  GeneralShardingMapping(uint64_t proclet_capacity, uint32_t max_shard_size);
  ~GeneralShardingMapping();
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_shards_in_range(std::optional<Key> l_key, std::optional<Key> r_key);
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_all_shards();
  std::optional<WeakProclet<Shard>> get_shard_for_key(std::optional<Key> key);
  void reserve_new_shard();
  WeakProclet<Shard> create_new_shard(std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      uint64_t container_capacity);
  void inc_ref_cnt();
  void dec_ref_cnt();
  void seal();
  void unseal();

 private:
  WeakProclet<GeneralShardingMapping> self_;
  uint64_t proclet_capacity_;
  uint32_t max_shard_size_;
  std::multimap<std::optional<Key>, Proclet<Shard>> mapping_;
  ReaderWriterLock rw_lock_;
  uint32_t ref_cnt_;
  CondVar ref_cnt_cv_;
  std::stack<Proclet<Shard>> reserved_shards_;
  Mutex mutex_;
};

}  // namespace nu

#include "nu/impl/sharding_mapping.ipp"

#pragma once

#include <map>
#include <optional>
#include <vector>

#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/reader_writer_lock.hpp"
#include "nu/shard.hpp"

namespace nu {

template <class Shard>
class GeneralShardingMapping {
 public:
  using Key = Shard::Key;

  GeneralShardingMapping();
  ~GeneralShardingMapping();
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_shards_in_range(std::optional<Key> l_key, std::optional<Key> r_key);
  std::optional<WeakProclet<Shard>> get_shard_for_key(std::optional<Key> key);
  void update_mapping(std::optional<Key> k, Proclet<Shard> shard);
  void inc_ref_cnt();
  void dec_ref_cnt();
  void seal();
  void unseal();

 private:
  std::map<std::optional<Key>, Proclet<Shard>> mapping_;
  ReaderWriterLock rw_lock_;
  uint32_t ref_cnt_;
  Mutex ref_cnt_mu_;
  CondVar ref_cnt_cv_;
};

}  // namespace nu

#include "nu/impl/sharding_mapping.ipp"

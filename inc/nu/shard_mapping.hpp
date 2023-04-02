#pragma once

#include <boost/circular_buffer.hpp>
#include <map>
#include <optional>
#include <stack>
#include <unordered_map>
#include <variant>
#include <vector>

#include "nu/shard.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <class Shard>
struct LogEntry {
  enum { kDelete, kInsert };
  uint8_t op;
  std::optional<typename Shard::Key> l_key;
  WeakProclet<Shard> shard;
  uint64_t seq;

  template <class Archive>
  void serialize(Archive &ar);
};

template <class Shard>
class Log {
 public:
  Log(uint32_t size);
  std::optional<std::vector<LogEntry<Shard>>> from(uint64_t start_seq);
  void append(uint8_t op, std::optional<typename Shard::Key> l_key,
              WeakProclet<Shard> shard);
  uint64_t last_seq() const;

 private:
  uint64_t seq_;
  boost::circular_buffer<LogEntry<Shard>> cb_;
};

template <class Shard>
class GeneralShardMapping {
 public:
  using Key = Shard::Key;
  using LogUpdates = std::vector<LogEntry<Shard>>;
  using Snapshot =
      std::pair<uint64_t,
                std::multimap<std::optional<Key>, WeakProclet<Shard>>>;

  GeneralShardMapping(uint32_t max_shard_bytes,
                      std::optional<uint32_t> max_shard_cnt, bool service);
  ~GeneralShardMapping();
  std::variant<LogUpdates, Snapshot> get_updates(uint64_t start_seq);
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_all_keys_and_shards();
  WeakProclet<Shard> get_shard_for_key(std::optional<Key> key);
  template <typename... As>
  WeakProclet<Shard> create_new_shard(std::optional<Key> l_key,
                                      std::optional<Key> r_key, As... args);
  WeakProclet<Shard> create_or_reuse_new_shard_for_init(
      std::optional<Key> l_key, NodeIP ip);
  bool delete_shard(std::optional<Key> l_key, WeakProclet<Shard> shard,
                    bool merge_left, NodeIP ip);
  void concat(WeakProclet<GeneralShardMapping> tail) requires(
      Shard::GeneralContainer::kContiguousIterator);
  void inc_ref_cnt();
  void dec_ref_cnt();
  void seal();
  void unseal();

 private:
  constexpr static double kProcletOverprovisionFactor = 3;
  constexpr static uint32_t kLogSize = 256;
  constexpr static uint32_t kCreateLocalShardThresh = 256;

  WeakProclet<GeneralShardMapping> self_;
  uint32_t max_shard_bytes_;
  uint32_t proclet_capacity_;
  std::optional<uint32_t> max_shard_cnt_;
  std::multimap<std::optional<Key>, Proclet<Shard>> mapping_;
  std::map<std::optional<Key>, Proclet<Shard>> stash_mapping_;
  uint32_t pending_creations_;
  uint32_t ref_cnt_;
  CondVar ref_cnt_cv_;
  CondVar oos_cv_;
  std::unordered_map<NodeIP, std::stack<Proclet<Shard>>> deleted_shards_;
  Log<Shard> log_;
  bool service_;
  Mutex mutex_;

  bool out_of_shards();
  std::vector<Proclet<Shard>> move_all_shards();
  Snapshot get_snapshot(const ScopedLock<Mutex> &lock);
};

}  // namespace nu

#include "nu/impl/shard_mapping.ipp"

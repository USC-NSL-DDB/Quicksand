#pragma once

#include <boost/circular_buffer.hpp>
#include <map>
#include <optional>
#include <set>
#include <stack>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "nu/shard.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

namespace nu {

template <class Shard>
struct LogEntry {
  enum { kMergeLeft, kMergeRight, kInsert };
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
                      std::optional<NodeIP> pinned_ip);
  ~GeneralShardMapping();
  std::variant<LogUpdates, Snapshot> get_updates(uint64_t client_seq);
  std::vector<std::pair<std::optional<Key>, WeakProclet<Shard>>>
  get_all_keys_and_shards();
  WeakProclet<Shard> get_shard_for_key(std::optional<Key> key);
  template <typename... As>
  WeakProclet<Shard> create_new_shard(std::optional<Key> l_key,
                                      std::optional<Key> r_key, As... args);
  std::pair<bool, WeakProclet<Shard>> create_or_reuse_new_shard_for_init(
      std::optional<Key> l_key, NodeIP ip);
  bool delete_shard(std::optional<Key> l_key, WeakProclet<Shard> shard,
                    bool merge_left, NodeIP ip, std::optional<float> cpu_load);
  void concat(WeakProclet<GeneralShardMapping> tail)
    requires(Shard::GeneralContainer::kContiguousIterator);
  void seal();
  void unseal();
  void client_register(uint64_t seq);
  void client_unregister(uint64_t seq);

 private:
  constexpr static double kProcletOverprovisionFactor = 3;
  constexpr static uint32_t kLogSize = 256;
  constexpr static uint32_t kGCIntervalUs = 100 * kOneMilliSecond;

  struct ShardWithLifetime {
    Proclet<Shard> shard;
    uint64_t start_seq;
    uint64_t end_seq;
  };

  WeakProclet<GeneralShardMapping> self_;
  uint32_t max_shard_bytes_;
  uint32_t proclet_capacity_;
  std::optional<NodeIP> pinned_ip_;
  std::multimap<std::optional<Key>, ShardWithLifetime> mapping_;
  std::multiset<uint64_t> client_seqs_;
  uint32_t ref_cnt_;
  CondVar ref_cnt_cv_;
  std::unordered_map<NodeIP, std::stack<Proclet<Shard>>> shards_to_reuse_;
  std::list<ShardWithLifetime> shards_to_gc_;
  Log<Shard> log_;
  uint64_t last_gc_us_;
  Future<void> shard_destruction_;
  Mutex mutex_;

  bool out_of_shards();
  std::vector<Proclet<Shard>> move_all_shards();
  Snapshot get_snapshot(const ScopedLock<Mutex> &lock);
  void check_gc_locked();
  void gc();
};

}  // namespace nu

#include "nu/impl/shard_mapping.ipp"

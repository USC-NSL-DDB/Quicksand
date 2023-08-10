#pragma once

#include <functional>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "nu/container.hpp"
#include "nu/rem_unique_ptr.hpp"
#include "nu/type_traits.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/read_skewed_lock.hpp"
#include "nu/utils/rob_executor.hpp"
#include "nu/utils/thread.hpp"

namespace nu {

template <class Shard>
class GeneralShardMapping;

template <GeneralContainerBased Container>
class GeneralShard;

template <GeneralContainerBased Container>
class ContainerHandle;

template <class Container>
struct ContainerAndMetadata;

template <GeneralContainerBased Container>
class GeneralShard;

template <class T>
concept GeneralShardBased = requires {
  requires is_base_of_template_v<T, GeneralShard>;
};

template <typename T, BoolIntegral Stateful>
class Service;

template <GeneralContainerBased Container>
class GeneralShard {
 public:
  using Key = Container::Key;
  using Val = Container::Val;
  using IterVal = Container::IterVal;
  using DataEntry = Container::DataEntry;
  using ShardMapping = GeneralShardMapping<GeneralShard>;
  using GeneralContainer = Container;
  using ContainerImpl = Container::Implementation;
  using ConstIterator = Container::ConstIterator;
  using ConstReverseIterator = Container::ConstReverseIterator;
  constexpr static bool kIsService = is_specialization_of_v<Container, Service>;

  struct ReqBatch {
    uint64_t mapping_seq;
    std::optional<Key> l_key;
    std::optional<Key> r_key;
    std::vector<Val> push_back_reqs;
    std::vector<DataEntry> insert_reqs;

    bool empty() const;
    template <class Archive>
    void serialize(Archive &ar);
  };

  GeneralShard(WeakProclet<ShardMapping> mapping, uint32_t max_shard_bytes);
  template <typename... As>
  GeneralShard(WeakProclet<ShardMapping> mapping, uint32_t max_shard_bytes,
               std::optional<Key> l_key, std::optional<Key> r_key, As... args);
  ~GeneralShard();
  void init_range_and_data(
      std::optional<Key> l_key, std::optional<Key> r_key,
      ContainerAndMetadata<Container> container_and_metadata);
  bool try_insert(DataEntry entry) requires InsertAble<Container>;
  bool try_push_back(std::optional<Key> l_key, std::optional<Key> r_key,
                     Val v) requires PushBackAble<Container>;
  std::optional<Val> try_front(
      std::optional<Key> l_key,
      std::optional<Key> r_key) requires HasFront<Container>;
  bool try_push_front(std::optional<Key> l_key, std::optional<Key> r_key,
                      Val v) requires PushFrontAble<Container>;
  std::optional<Val> try_pop_front(
      std::optional<Key> l_key,
      std::optional<Key> r_key) requires TryPopFrontAble<Container>;
  std::optional<std::vector<Val>> try_pop_front_nb(
      std::optional<Key> l_key, std::optional<Key> r_key,
      std::size_t num) requires TryPopFrontAble<Container>;
  std::optional<Val> try_back(
      std::optional<Key> l_key,
      std::optional<Key> r_key) requires HasBack<Container>;
  std::optional<Val> try_pop_back(
      std::optional<Key> l_key,
      std::optional<Key> r_key) requires TryPopBackAble<Container>;
  std::optional<std::vector<Val>> try_pop_back_nb(
      std::optional<Key> l_key, std::optional<Key> r_key,
      std::size_t num) requires TryPopBackAble<Container>;
  std::optional<ReqBatch> try_handle_batch(ReqBatch &batch);
  std::pair<bool, std::optional<IterVal>> find_data(
      Key k) requires FindDataAble<Container>;
  std::pair<IterVal, ConstIterator> find(Key k) requires FindAble<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_front_block_with_iters(
      uint32_t block_size) requires ConstIterable<Container>;
  std::pair<std::vector<IterVal>, ConstIterator> get_front_block(
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_rfront_block_with_iters(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::pair<std::vector<IterVal>, ConstReverseIterator> get_rfront_block(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_back_block_with_iters(
      uint32_t block_size) requires ConstIterable<Container>;
  std::pair<std::vector<IterVal>, ConstIterator> get_back_block(
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_rback_block_with_iters(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::pair<std::vector<IterVal>, ConstReverseIterator> get_rback_block(
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_next_block_with_iters(
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<IterVal> get_next_block(
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstIterator>> get_prev_block_with_iters(
      ConstIterator succ_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<IterVal> get_prev_block(
      ConstIterator succ_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_next_rblock_with_iters(
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<IterVal> get_next_rblock(
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<std::pair<IterVal, ConstReverseIterator>>
  get_prev_rblock_with_iters(
      ConstReverseIterator succ_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  std::vector<IterVal> get_prev_rblock(
      ConstReverseIterator succ_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  ConstIterator cbegin() requires ConstIterable<Container>;
  ConstIterator clast() requires ConstIterable<Container>;
  ConstIterator cend() requires ConstIterable<Container>;
  ConstReverseIterator crbegin() requires ConstReverseIterable<Container>;
  ConstReverseIterator crlast() requires ConstReverseIterable<Container>;
  ConstReverseIterator crend() requires ConstReverseIterable<Container>;
  bool empty();
  std::size_t size();
  std::optional<IterVal> find_data_by_order(
      std::size_t order) requires FindAbleByOrder<Container>;
  Container get_container_copy();
  ContainerHandle<Container> get_container_handle();
  Key split_at_end() requires GeneralContainer::kContiguousIterator;
  Key rebase(Key new_l_key) requires GeneralContainer::kContiguousIterator;
  template <typename RetT, typename... S0s>
  std::conditional_t<std::is_void_v<RetT>, bool, std::optional<RetT>>
  try_compute_on(Key k, uintptr_t fn_addr, S0s... states);
  bool try_update_key(bool update_left, std::optional<Key> new_key);
  std::optional<bool> try_erase(Key k) requires EraseAble<Container>;
  template <typename RetT, typename... S0s>
  std::conditional_t<std::is_void_v<RetT>, bool, std::optional<RetT>> try_run(
      Key k, uintptr_t fn_addr, S0s... states);

 private:
  constexpr static uint32_t kReserveProbeSize =
      std::max(1UL, 65536 / sizeof(DataEntry));
  constexpr static float kReserveContainerSizeRatio = 0.5;
  constexpr static float kAlmostFullThresh = 0.95;
  constexpr static uint32_t kSlabFragmentationHeadroom = 2 << 20;
  constexpr static float kComputeLoadHighThresh = 1.0;
  constexpr static float kComputeLoadLowThresh = 0.5;

  const uint32_t max_shard_bytes_;
  uint32_t real_max_shard_bytes_;
  WeakProclet<ShardMapping> mapping_;
  std::optional<Key> l_key_;
  std::optional<Key> r_key_;
  Container container_;
  ReadSkewedLock rw_lock_;
  SlabAllocator *slab_;
  CPULoad *cpu_load_;
  uint64_t container_bucket_size_;
  uint64_t initial_slab_usage_;
  std::size_t initial_size_;
  std::size_t size_thresh_;
  Mutex empty_mutex_;
  CondVar empty_cv_;
  bool deleted_;
  bool cofounder_;
  WeakProclet<GeneralShard> self_;
  Thread compute_monitor_th_;

  friend class ContainerHandle<Container>;
  template <GeneralShardBased S>
  friend class ContiguousDSRangeImpl;

  void split();
  bool should_split(std::size_t size) const;
  void split_with_reader_lock();
  void try_delete_self_with_reader_lock(bool merge_left);
  bool try_compute_delete_self();
  void compute_split();
  bool should_reject(const std::optional<Key> &l_key,
                     const std::optional<Key> &r_key);
  bool should_reject(const std::optional<Key> &k);
  uint32_t __get_next_block_with_iters(
      std::vector<std::pair<IterVal, ConstIterator>>::iterator block_iter,
      ConstIterator prev_iter,
      uint32_t block_size) requires ConstIterable<Container>;
  uint32_t __get_next_rblock_with_iters(
      std::vector<std::pair<IterVal, ConstReverseIterator>>::iterator block_it,
      ConstReverseIterator prev_iter,
      uint32_t block_size) requires ConstReverseIterable<Container>;
  void start_compute_monitor_th();
};

template <GeneralContainerBased Container>
class ContainerHandle {
 public:
  ContainerHandle(Container *c, GeneralShard<Container> *shard);
  ~ContainerHandle();
  Container *operator->();
  Container &operator*();

 private:
  Container *c_;
  GeneralShard<Container> *shard_;
};

template <class Container>
struct ContainerAndMetadata {
  Container container;
  std::size_t capacity = 0;
  uint64_t container_bucket_size = 0;

  ContainerAndMetadata() = default;
  ContainerAndMetadata(const ContainerAndMetadata &);
  ContainerAndMetadata(ContainerAndMetadata &&) = default;
  ContainerAndMetadata &operator=(ContainerAndMetadata &&) = default;

  template <class Archive>
  void save(Archive &ar) const;

  template <class Archive>
  void load(Archive &ar);
};

}  // namespace nu

#include "nu/impl/shard.ipp"

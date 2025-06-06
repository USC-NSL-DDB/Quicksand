#include <cmath>
#include <experimental/scope>

#include "nu/shard_mapping.hpp"
#include "nu/utils/caladan.hpp"
#include "nu/utils/rob_executor.hpp"
#include "nu/utils/scoped_lock.hpp"
#include "nu/utils/time.hpp"

namespace nu {

template <GeneralContainerBased Container>
inline ContainerHandle<Container>::ContainerHandle(
    Container *c, GeneralShard<Container> *shard)
    : c_(c), shard_(shard) {
  shard_->rw_lock_.reader_lock();
}

template <GeneralContainerBased Container>
inline ContainerHandle<Container>::~ContainerHandle() {
  shard_->rw_lock_.reader_unlock();
}

template <GeneralContainerBased Container>
inline ContainerHandle<Container>::operator bool() const {
  return c_;
}

template <GeneralContainerBased Container>
inline Container *ContainerHandle<Container>::operator->() {
  return c_;
}

template <GeneralContainerBased Container>
inline Container &ContainerHandle<Container>::operator*() {
  return *c_;
}

template <class Container>
template <class Archive>
inline void ContainerAndMetadata<Container>::save_move(Archive &ar) {
  ar(capacity, *container, container_bucket_size);
  container.reset();
}

template <class Container>
template <class Archive>
inline void ContainerAndMetadata<Container>::load(Archive &ar) {
  ar(capacity);
  container = std::make_unique<Container>();
  if constexpr (Reservable<Container>) {
    container->reserve(capacity);
  }
  ar(*container, container_bucket_size);
}

template <class Container>
inline ContainerAndMetadata<Container>::ContainerAndMetadata(
    const ContainerAndMetadata &o)
    : container(std::make_unique<Container>()),
      capacity(o.capacity),
      container_bucket_size(o.container_bucket_size) {
  if constexpr (Reservable<Container>) {
    container->reserve(capacity);
  }
  *container = *o.container;
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::ReqBatch::empty() const {
  return push_back_reqs.empty() && insert_reqs.empty();
}

template <GeneralContainerBased Container>
template <class Archive>
inline void GeneralShard<Container>::ReqBatch::serialize(Archive &ar) {
  ar(mapping_seq, l_key, r_key, push_back_reqs, insert_reqs);
}

template <GeneralContainerBased Container>
inline GeneralShard<Container>::GeneralShard(WeakProclet<ShardMapping> mapping,
                                             uint32_t max_shard_bytes)
    : max_shard_bytes_(max_shard_bytes),
      real_max_shard_bytes_(max_shard_bytes / kAlmostFullThresh),
      mapping_(std::move(mapping)),
      deleted_(true),
      cofounder_(false) {
  ProcletHeader *proclet_header;
  {
    Caladan::PreemptGuard g;
    auto *runtime = get_runtime();
    proclet_header = runtime->get_current_proclet_header();
    self_ = runtime->get_current_weak_proclet<GeneralShard>();
  }
  slab_ = &proclet_header->slab;
  cpu_load_ = &proclet_header->cpu_load;
}

template <GeneralContainerBased Container>
template <typename... As>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardMapping> mapping,
                                      uint32_t max_shard_bytes,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key, As... args)
    : max_shard_bytes_(max_shard_bytes),
      real_max_shard_bytes_(max_shard_bytes / kAlmostFullThresh),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      container_(std::make_unique<Container>(std::move(args)...)),
      deleted_(false),
      cofounder_(true) {
  ProcletHeader *proclet_header;
  {
    Caladan::PreemptGuard g;
    auto *runtime = get_runtime();
    proclet_header = runtime->get_current_proclet_header();
    self_ = runtime->get_current_weak_proclet<GeneralShard>();
  }
  slab_ = &proclet_header->slab;
  cpu_load_ = &proclet_header->cpu_load;

  if constexpr (Reservable<Container>) {
    auto old_slab_usage = slab_->get_cur_usage();
    container_->reserve(kReserveProbeSize);
    auto new_slab_usage = slab_->get_cur_usage();
    container_bucket_size_ =
        std::ceil(static_cast<float>(new_slab_usage - old_slab_usage) /
                  kReserveProbeSize);
    auto reserve_size =
        max_shard_bytes * kReserveContainerSizeRatio / container_bucket_size_;
    container_->reserve(reserve_size);
    initial_slab_usage_ = slab_->get_cur_usage();
    initial_size_ = 0;
    initial_capacity_ = container_->capacity();
    size_thresh_ = kAlmostFullThresh * reserve_size;
  }

  if constexpr (kIsService) {
    start_compute_monitor_th();
  }
}

template <GeneralContainerBased Container>
inline GeneralShard<Container>::~GeneralShard() {
  deleted_ = true;
  barrier();
  if (compute_monitor_th_.joinable()) {
    compute_monitor_th_.join();
  }
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::init_range(std::optional<Key> l_key,
                                         std::optional<Key> r_key) {
  if constexpr (kIsStatelessService) {
    split_merge_mutex_.lock();
  } else {
    rw_lock_.writer_lock();
  }

  l_key_ = l_key;
  r_key_ = r_key;
  deleted_ = false;

  if constexpr (kIsStatelessService) {
    split_merge_mutex_.unlock();
  } else {
    rw_lock_.writer_unlock();
  }

  if constexpr (kIsService) {
    start_compute_monitor_th();
  }
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::init_range_and_data(
    std::optional<Key> l_key, std::optional<Key> r_key,
    ContainerAndMetadata<Container> container_and_metadata) {
  if constexpr (kIsStatelessService) {
    split_merge_mutex_.lock();
  } else {
    rw_lock_.writer_lock();
  }

  l_key_ = l_key;
  r_key_ = r_key;
  container_ = std::move(container_and_metadata.container);
  initial_slab_usage_ = slab_->get_cur_usage();
  initial_size_ = container_->size();
  if constexpr (HasCapacity<Container>) {
    initial_capacity_ = container_->capacity();
  }
  container_bucket_size_ = container_and_metadata.container_bucket_size;
  real_max_shard_bytes_ = max_shard_bytes_ / kAlmostFullThresh;
  size_thresh_ = kAlmostFullThresh * container_and_metadata.capacity;
  deleted_ = false;

  if constexpr (kIsStatelessService) {
    split_merge_mutex_.unlock();
  } else {
    rw_lock_.writer_unlock();
  }

  if constexpr (kIsService) {
    start_compute_monitor_th();
  }
}

template <GeneralContainerBased Container>
inline Container GeneralShard<Container>::get_container_copy() {
  rw_lock_.reader_lock();

  RuntimeSlabGuard slab_guard;

  Container c = *container_;
  rw_lock_.reader_unlock();

  return c;
}

template <GeneralContainerBased Container>
inline ContainerHandle<Container>
GeneralShard<Container>::get_container_handle() {
  return ContainerHandle<Container>(container_.get(), this);
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::split() {
  Key mid_k;
  std::size_t new_container_capacity = 0;

  if constexpr (Reservable<Container>) {
    auto inflight_thread_cnts =
        to_proclet_header(self_.get_id())->thread_cnt.get();
    if (inflight_thread_cnts < kSplitHighConcurrencyThresh) {
      auto diff_data_size =
          static_cast<int64_t>(slab_->get_cur_usage()) - initial_slab_usage_;
      auto diff_size = static_cast<int64_t>(container_->size()) - initial_size_;
      auto data_size =
          std::max(0.0f, static_cast<float>(diff_data_size) / diff_size);
      new_container_capacity =
          max_shard_bytes_ / (data_size + container_bucket_size_);
    } else {
      // Too many inflight threads now so that the mem usage might be heavily
      // under-reported, resulting in a bad capacity estimation. Thus, let's
      // just use the same capacity of the parent shard.
      new_container_capacity = initial_capacity_;
    }
  }

  {
    MigrationGuard migration_guard;
    std::unique_ptr<Container> latter_half_container;

    BUG_ON(container_->empty());
    if constexpr (kIsStatelessService) {
      mid_k = l_key_.value_or(std::numeric_limits<Key>::min()) / 2 +
              r_key_.value_or(std::numeric_limits<Key>::max()) / 2;
    } else {
      RuntimeSlabGuard slab_guard;
      latter_half_container.reset(new Container());
      container_->split(&mid_k, latter_half_container.get());
    }

    // The if below avoids creating an illegal empty new shard, which could
    // happen, e.g.,, when splitting a non-edge queue shard.
    if (likely(mid_k != r_key_ || mid_k == l_key_)) {
      auto [reuse, new_shard] =
          mapping_.run(&ShardMapping::create_or_reuse_new_shard_for_init, mid_k,
                       nu::get_runtime()->caladan()->get_ip());
      if (reuse && kIsStatelessService) {
        new_shard.run(&GeneralShard::init_range, mid_k, r_key_);
      } else {
        if constexpr (kIsStatelessService) {
          RuntimeSlabGuard slab_guard;
          latter_half_container.reset(new Container(*container_));
        }
        ContainerAndMetadata<Container> container_and_metadata;
        container_and_metadata.capacity = new_container_capacity;
        container_and_metadata.container = std::move(latter_half_container);
        container_and_metadata.container_bucket_size = container_bucket_size_;
        new_shard.run(&GeneralShard::init_range_and_data, mid_k, r_key_,
                      std::move(container_and_metadata));
      }
      r_key_ = mid_k;
    }
  }

  auto new_slab_usage = slab_->get_cur_usage();
  if (unlikely(new_slab_usage > real_max_shard_bytes_)) {
    // Grant slightly more memory to incorporate fragmentations in our slab
    // allocator.
    real_max_shard_bytes_ = new_slab_usage + kSlabFragmentationHeadroom;
  }
  if constexpr (Reservable<Container>) {
    size_thresh_ = std::max(size_thresh_, container_->size());
  }
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::should_reject(
    const std::optional<Key> &l_key, const std::optional<Key> &r_key) {
  return deleted_ || (l_key < l_key_) || (r_key_ && (r_key > r_key_ || !r_key));
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::should_reject(
    const std::optional<Key> &k) {
  assert(k.has_value());
  return deleted_ || (k < l_key_) || (r_key_ && k > r_key_);
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::should_split(std::size_t size) const {
  // This is possible when there's a burst of ongoing insertions.
  if (unlikely(!size)) {
    return false;
  }

  bool over_container_size = false;
  bool over_slab_size = false;

  if constexpr (Reservable<Container>) {
    over_container_size = (size > size_thresh_);
  }

  over_slab_size = (slab_->get_cur_usage() > real_max_shard_bytes_);

  return over_container_size || over_slab_size;
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::split_with_reader_lock() {
  rw_lock_.reader_unlock();
  if (rw_lock_.writer_lock_if(
          [&] { return should_split(container_->size()); })) {
    split();
    rw_lock_.writer_unlock();
  }
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::compute_split() {
  if constexpr (kIsStatelessService) {
    split_merge_mutex_.lock();
  } else {
    rw_lock_.writer_lock();
  }

  split();
  cpu_load_->halve();

  if constexpr (kIsStatelessService) {
    split_merge_mutex_.unlock();
  } else {
    rw_lock_.writer_unlock();
  }
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::try_delete_self_with_reader_lock(
    bool merge_left) {
  rw_lock_.reader_unlock();
  rw_lock_.writer_lock();
  if (container_->empty() && !deleted_) {
    if (likely(mapping_.run(&ShardMapping::delete_shard, l_key_, self_,
                            merge_left, Caladan::get_ip(),
                            std::optional<float>()))) {
      deleted_ = true;
    }
  }
  rw_lock_.writer_unlock();
}

template <GeneralContainerBased Container>
bool GeneralShard<Container>::try_compute_delete_self(float cpu_load) {
  bool succeed = false;

  if constexpr (kIsStatelessService) {
    split_merge_mutex_.lock();
  } else {
    rw_lock_.writer_lock();
  }

  if (likely(mapping_.run(&ShardMapping::delete_shard, l_key_, self_,
                          /* merge_left = */ true, Caladan::get_ip(),
                          cpu_load))) {
    succeed = deleted_ = true;
  }

  if constexpr (kIsStatelessService) {
    split_merge_mutex_.unlock();
  } else {
    rw_lock_.writer_unlock();
  }

  return succeed;
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::try_insert(DataEntry entry)
  requires InsertAble<Container>
{
  rw_lock_.reader_lock();

  bool rejected;
  if constexpr (HasVal<Container>) {
    rejected = should_reject(entry.first);
  } else {
    rejected = should_reject(entry);
  }

  if (unlikely(rejected)) {
    rw_lock_.reader_unlock();
    return false;
  }

  std::size_t size;
  if constexpr (HasVal<Container>) {
    size = container_->insert(std::move(entry.first), std::move(entry.second));
  } else {
    size = container_->insert(std::move(entry));
  }

  if (unlikely(should_split(size))) {
    split_with_reader_lock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::try_push_back(std::optional<Key> l_key,
                                                   std::optional<Key> r_key,
                                                   Val v)
  requires PushBackAble<Container>
{
  rw_lock_.reader_lock();

  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return false;
  }

  auto size = container_->push_back(std::move(v));

  if (unlikely(size == 1)) {
    ScopedLock lock(&empty_mutex_);

    empty_cv_.signal_all();
  }

  if (unlikely(should_split(size))) {
    split_with_reader_lock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::try_push_front(std::optional<Key> l_key,
                                                    std::optional<Key> r_key,
                                                    Val v)
  requires PushFrontAble<Container>
{
  rw_lock_.reader_lock();
  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return false;
  }

  auto size = container_->push_front(std::move(v));

  if (unlikely(size == 1)) {
    ScopedLock lock(&empty_mutex_);

    empty_cv_.signal_all();
  }

  if (unlikely(should_split(size))) {
    split_with_reader_lock();
    return true;
  }

  rw_lock_.reader_unlock();
  return true;
}

template <GeneralContainerBased Container>
inline std::optional<typename Container::Val>
GeneralShard<Container>::try_front(std::optional<Key> l_key,
                                   std::optional<Key> r_key)
  requires HasFront<Container>
{
  rw_lock_.reader_lock();
  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  Val v = container_->front();
  rw_lock_.reader_unlock();
  return v;
}

template <GeneralContainerBased Container>
inline std::optional<typename Container::Val>
GeneralShard<Container>::try_pop_front(std::optional<Key> l_key,
                                       std::optional<Key> r_key)
  requires TryPopFrontAble<Container>
{
  rw_lock_.reader_lock();

retry:
  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  auto front = container_->try_pop_front(1);
  if (unlikely(front.empty())) {
    if (r_key_) {
      try_delete_self_with_reader_lock(/* merge_left = */ false);
      return std::nullopt;
    } else {
      ScopedLock lock(&empty_mutex_);

      while (container_->empty()) {
        empty_cv_.wait(&empty_mutex_);
      }
      goto retry;
    }
  }

  rw_lock_.reader_unlock();

  return front.front();
}

template <GeneralContainerBased Container>
inline std::optional<std::vector<typename Container::Val>>
GeneralShard<Container>::try_pop_front_nb(std::optional<Key> l_key,
                                          std::optional<Key> r_key,
                                          std::size_t num)
  requires TryPopFrontAble<Container>
{
  rw_lock_.reader_lock();

  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  auto front_elems = container_->try_pop_front(num);
  if (unlikely(front_elems.size() < num && r_key_)) {
    try_delete_self_with_reader_lock(/* merge_left = */ false);
  } else {
    rw_lock_.reader_unlock();
  }

  return front_elems;
}

template <GeneralContainerBased Container>
inline std::optional<typename Container::Val> GeneralShard<Container>::try_back(
    std::optional<Key> l_key, std::optional<Key> r_key)
  requires HasBack<Container>
{
  rw_lock_.reader_lock();
  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  Val v = container_->back();
  rw_lock_.reader_unlock();
  return v;
}

template <GeneralContainerBased Container>
inline std::optional<typename Container::Val>
GeneralShard<Container>::try_pop_back(std::optional<Key> l_key,
                                      std::optional<Key> r_key)
  requires TryPopBackAble<Container>
{
  rw_lock_.reader_lock();

retry:
  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  auto back = container_->try_pop_back(1);
  if (unlikely(back.empty())) {
    if (l_key_) {
      try_delete_self_with_reader_lock(/* merge_left = */ true);
      return std::nullopt;
    } else {
      ScopedLock lock(&empty_mutex_);

      while (container_->empty()) {
        empty_cv_.wait(&empty_mutex_);
      }
      goto retry;
    }
  }

  rw_lock_.reader_unlock();

  return back.front();
}

template <GeneralContainerBased Container>
inline std::optional<std::vector<typename Container::Val>>
GeneralShard<Container>::try_pop_back_nb(std::optional<Key> l_key,
                                         std::optional<Key> r_key,
                                         std::size_t num)
  requires TryPopBackAble<Container>
{
  rw_lock_.reader_lock();

  if (unlikely(should_reject(l_key, r_key))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  auto back_elems = container_->try_pop_back(num);
  if (unlikely(back_elems.size() < num && l_key_)) {
    try_delete_self_with_reader_lock(/* merge_left = */ true);
  } else {
    rw_lock_.reader_unlock();
  }

  return back_elems;
}

template <GeneralContainerBased Container>
std::optional<typename GeneralShard<Container>::ReqBatch>
GeneralShard<Container>::try_handle_batch(ReqBatch &batch) {
  rw_lock_.reader_lock();
  if (unlikely(should_reject(batch.l_key, batch.r_key))) {
    rw_lock_.reader_unlock();
    return std::move(batch);
  }

  auto cond = [&](std::size_t size) { return !should_split(size); };

  if constexpr (PushBackAble<Container>) {
    if (!batch.push_back_reqs.empty()) {
      auto [succeed, prev_empty] =
          container_->push_back_batch_if(cond, batch.push_back_reqs);
      if (unlikely(!succeed)) {
        split_with_reader_lock();
        return std::move(batch);
      } else if (unlikely(prev_empty)) {
        ScopedLock lock(&empty_mutex_);

        empty_cv_.signal_all();
      }
    }
  }

  if constexpr (InsertAble<Container>) {
    if (!batch.insert_reqs.empty()) {
      if (unlikely(!container_->insert_batch_if(cond, batch.insert_reqs))) {
        split_with_reader_lock();
        return std::move(batch);
      }
    }
  }

  rw_lock_.reader_unlock();
  return std::nullopt;
}

template <GeneralContainerBased Container>
inline std::pair<bool, std::optional<typename Container::IterVal>>
GeneralShard<Container>::find_data(Key k)
  requires FindDataAble<Container>
{
  rw_lock_.reader_lock();

  if (unlikely(should_reject(k))) {
    rw_lock_.reader_unlock();
    return std::make_pair(false, std::nullopt);
  }

  auto iter_val = container_->find_data(std::move(k));

  rw_lock_.reader_unlock();
  return std::make_pair(true, std::move(iter_val));
}

template <GeneralContainerBased Container>
inline std::pair<typename Container::IterVal, typename Container::ConstIterator>
GeneralShard<Container>::find(Key k)
  requires FindAble<Container>
{
  // Currently, the invocation happens only after sealing the DS. Thus we can
  // bypass all the redundant checks.
  auto iter = container_->find(std::move(k));
  return std::make_pair(*iter, std::move(iter));
}

template <GeneralContainerBased Container>
inline std::optional<typename Container::IterVal>
GeneralShard<Container>::find_data_by_order(std::size_t order)
  requires FindAbleByOrder<Container>
{
  // Currently, the invocation happens only after sealing the DS. Thus we can
  // bypass all the redundant checks.
  auto iter = container_->find_by_order(order);
  if (unlikely(iter == container_->cend())) {
    return std::nullopt;
  }
  return *iter;
}

template <GeneralContainerBased Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_next_block_with_iters(ConstIterator prev_iter,
                                                   uint32_t block_size)
  requires ConstIterable<Container>
{
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size);
  auto size = __get_next_block_with_iters(block.begin(), prev_iter, block_size);
  block.resize(size);
  return block;
}

template <GeneralContainerBased Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_next_block(ConstIterator prev_iter,
                                        uint32_t block_size)
  requires ConstIterable<Container>
{
  ConstIterator end_iter;
  if (likely(container_->cend() - prev_iter > block_size + 1)) {
    end_iter = prev_iter + block_size + 1;
  } else {
    end_iter = container_->cend();
  }
  return std::vector<IterVal>(std::to_address(prev_iter + 1),
                              std::to_address(end_iter));
}

template <GeneralContainerBased Container>
uint32_t GeneralShard<Container>::__get_next_block_with_iters(
    std::vector<std::pair<IterVal, ConstIterator>>::iterator block_iter,
    ConstIterator prev_iter, uint32_t block_size)
  requires ConstIterable<Container>
{
  auto iter = prev_iter;

  uint32_t i;
  for (i = 0; i < block_size; ++i, ++block_iter) {
    if (unlikely(++iter == container_->cend())) {
      break;
    }
    *block_iter = std::pair(*iter, iter);
  }

  return i;
}

template <GeneralContainerBased Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_prev_block_with_iters(ConstIterator succ_iter,
                                                   uint32_t block_size)
  requires ConstIterable<Container>
{
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size);
  auto iter = succ_iter;

  uint32_t i;
  for (i = 0; i < block_size; i++) {
    if (unlikely(iter-- == container_->cbegin())) {
      break;
    }
    block[i] = std::pair(*iter, iter);
  }
  block.resize(i);

  std::reverse(block.begin(), block.end());
  return block;
}

template <GeneralContainerBased Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_prev_block(ConstIterator succ_iter,
                                        uint32_t block_size)
  requires ConstIterable<Container>
{
  ConstIterator begin_iter;
  if (likely(succ_iter - container_->cbegin() > block_size)) {
    begin_iter = succ_iter - block_size;
  } else {
    begin_iter = container_->cbegin();
  }
  return std::vector<IterVal>(std::to_address(begin_iter),
                              std::to_address(succ_iter));
}

template <GeneralContainerBased Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_next_rblock_with_iters(
    ConstReverseIterator prev_iter, uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size);
  auto size =
      __get_next_rblock_with_iters(block.begin(), prev_iter, block_size);
  block.resize(size);
  return block;
}

template <GeneralContainerBased Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_next_rblock(ConstReverseIterator prev_iter,
                                         uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  ConstReverseIterator end_iter;
  if (likely(container_->crend() - prev_iter > block_size + 1)) {
    end_iter = prev_iter + block_size + 1;
  } else {
    end_iter = container_->crend();
  }
  auto span =
      std::span(std::to_address(end_iter - 1), std::to_address(prev_iter));
  return std::vector<IterVal>(span.rbegin(), span.rend());
}

template <GeneralContainerBased Container>
uint32_t GeneralShard<Container>::__get_next_rblock_with_iters(
    std::vector<std::pair<IterVal, ConstReverseIterator>>::iterator block_iter,
    ConstReverseIterator prev_iter, uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  auto iter = prev_iter;

  uint32_t i;
  for (i = 0; i < block_size; ++i, ++block_iter) {
    if (unlikely(++iter == container_->crend())) {
      break;
    }
    *block_iter = std::pair(*iter, iter);
  }

  return i;
}

template <GeneralContainerBased Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_prev_rblock_with_iters(
    ConstReverseIterator succ_iter, uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size);
  auto iter = succ_iter;

  uint32_t i;
  for (i = 0; i < block_size; i++) {
    if (unlikely(iter-- == container_->crbegin())) {
      break;
    }
    block[i] = std::pair(*iter, iter);
  }
  block.resize(i);

  std::reverse(block.begin(), block.end());
  return block;
}

template <GeneralContainerBased Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_prev_rblock(ConstReverseIterator succ_iter,
                                         uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  ConstReverseIterator begin_iter;
  if (likely(succ_iter - container_->crbegin() > block_size)) {
    begin_iter = succ_iter - block_size;
  } else {
    begin_iter = container_->crbegin();
  }
  auto span = std::span(std::to_address(succ_iter - 1),
                        std::to_address(begin_iter - 1));
  return std::vector<IterVal>(span.rbegin(), span.rend());
}

template <GeneralContainerBased Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_front_block_with_iters(uint32_t block_size)
  requires ConstIterable<Container>
{
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size + 1);
  block[0] = std::pair(*container_->cbegin(), container_->cbegin());
  auto size = __get_next_block_with_iters(++block.begin(), container_->cbegin(),
                                          block_size);
  block.resize(size + 1);
  return block;
}

template <GeneralContainerBased Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstIterator>
GeneralShard<Container>::get_front_block(uint32_t block_size)
  requires ConstIterable<Container>
{
  ConstIterator end_iter;
  if (likely(container_->cend() - container_->cbegin() > block_size)) {
    end_iter = container_->cbegin() + block_size;
  } else {
    end_iter = container_->cend();
  }
  return std::make_pair(
      std::vector<IterVal>(std::to_address(container_->cbegin()),
                           std::to_address(end_iter)),
      container_->cbegin());
}

template <GeneralContainerBased Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rfront_block_with_iters(uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size + 1);
  block[0] = std::pair(*container_->crbegin(), container_->crbegin());
  auto size = __get_next_rblock_with_iters(++block.begin(),
                                           container_->crbegin(), block_size);
  block.resize(size + 1);
  return block;
}

template <GeneralContainerBased Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstReverseIterator>
GeneralShard<Container>::get_rfront_block(uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  ConstReverseIterator end_iter;
  if (likely(container_->crend() - container_->crbegin() > block_size)) {
    end_iter = container_->crbegin() + block_size;
  } else {
    end_iter = container_->crend();
  }
  auto span = std::span(std::to_address(end_iter - 1),
                        std::to_address(container_->crbegin() - 1));
  return std::make_pair(std::vector<IterVal>(span.rbegin(), span.rend()),
                        container_->crbegin());
}

template <GeneralContainerBased Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_back_block_with_iters(uint32_t block_size)
  requires ConstIterable<Container>
{
  return get_prev_block_with_iters(container_->cend(), block_size);
}

template <GeneralContainerBased Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstIterator>
GeneralShard<Container>::get_back_block(uint32_t block_size)
  requires ConstIterable<Container>
{
  auto data = get_prev_block(container_->cend(), block_size);
  return std::make_pair(std::move(data), container_->cend() - data.size());
}

template <GeneralContainerBased Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rback_block_with_iters(uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  return get_prev_rblock_with_iters(container_->crend(), block_size);
}

template <GeneralContainerBased Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstReverseIterator>
GeneralShard<Container>::get_rback_block(uint32_t block_size)
  requires ConstReverseIterable<Container>
{
  auto data = get_prev_rblock(container_->crend(), block_size);
  return std::make_pair(std::move(data), container_->crend() - data.size());
}

template <GeneralContainerBased Container>
inline typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cbegin()
  requires ConstIterable<Container>
{
  return container_->cbegin();
}

template <GeneralContainerBased Container>
inline typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::clast()
  requires ConstIterable<Container>
{
  return --container_->cend();
}

template <GeneralContainerBased Container>
inline typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cend()
  requires ConstIterable<Container>
{
  return container_->cend();
}

template <GeneralContainerBased Container>
inline typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crbegin()
  requires ConstReverseIterable<Container>
{
  return container_->crbegin();
}

template <GeneralContainerBased Container>
inline typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crlast()
  requires ConstReverseIterable<Container>
{
  return --container_->crend();
}

template <GeneralContainerBased Container>
inline typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crend()
  requires ConstReverseIterable<Container>
{
  return container_->crend();
}

template <GeneralContainerBased Container>
inline bool GeneralShard<Container>::empty() {
  return container_->empty();
}

template <GeneralContainerBased Container>
inline std::size_t GeneralShard<Container>::size() {
  return container_->size();
}

template <GeneralContainerBased Container>
inline Container::Key GeneralShard<Container>::split_at_end()
  requires GeneralContainer::kContiguousIterator
{
  if (r_key_) {
    return *r_key_;
  }

  r_key_ = l_key_.value_or(0) + container_->size();
  return *r_key_;
}

template <GeneralContainerBased Container>
inline Container::Key GeneralShard<Container>::rebase(Key new_l_key)
  requires GeneralContainer::kContiguousIterator
{
  l_key_ = new_l_key;
  auto new_r_key = container_->rebase(new_l_key);
  if (r_key_) {
    r_key_ = new_r_key;
  }
  return new_r_key;
}

template <GeneralContainerBased Container>
template <typename RetT, typename... S0s>
std::conditional_t<std::is_void_v<RetT>, bool, std::optional<RetT>>
GeneralShard<Container>::try_compute_on(Key k, uintptr_t fn_addr,
                                        S0s... states) {
  auto fn = reinterpret_cast<RetT (*)(ContainerImpl &, S0s...)>(fn_addr);

  rw_lock_.reader_lock();
  auto rw_unlocker =
      std::experimental::scope_exit([&] { rw_lock_.reader_unlock(); });

  if (unlikely(should_reject(k))) {
    if constexpr (std::is_void_v<RetT>) {
      return false;
    } else {
      return std::nullopt;
    }
  }

  if constexpr (std::is_void_v<RetT>) {
    container_->compute(fn, std::move(states)...);
    return true;
  } else {
    return container_->compute(fn, std::move(states)...);
  }
}

template <GeneralContainerBased Container>
bool GeneralShard<Container>::try_merge(bool merge_left,
                                        std::optional<Key> new_key,
                                        std::optional<float> cpu_load) {
  bool succeed;
  if constexpr (kIsStatelessService) {
    succeed = split_merge_mutex_.try_lock();
  } else {
    succeed = rw_lock_.reader_try_lock();
  }
  if (unlikely(!succeed)) {
    return false;
  }

  bool oversized = cpu_load.has_value() &&
                   *cpu_load + cpu_load_->get_load() > kComputeLoadHighThresh;
  if (likely(!deleted_ && !oversized)) {
    if (merge_left) {
      l_key_ = new_key;
    } else {
      r_key_ = new_key;
    }

    cpu_load_->inc(cpu_load.value_or(0));
  } else {
    succeed = false;
  }

  if constexpr (kIsStatelessService) {
    split_merge_mutex_.unlock();
  } else {
    rw_lock_.reader_unlock();
  }

  return succeed;
}

template <GeneralContainerBased Container>
void GeneralShard<Container>::start_compute_monitor_th() {
  if (compute_monitor_th_.joinable()) {
    compute_monitor_th_.join();
  }

  compute_monitor_th_ = Thread([&] {
    while (!Caladan::access_once(deleted_)) {
      Time::sleep(CPULoad::kDecayIntervalUs);
      auto cpu_load = cpu_load_->get_load();
      if (cpu_load > kComputeLoadHighThresh) {
        compute_split();
      } else if (cpu_load < kComputeLoadLowThresh) {
        if (!cofounder_ && try_compute_delete_self(cpu_load)) {
          break;
        }
      }
    }
  });
}

template <GeneralContainerBased Container>
std::optional<bool> GeneralShard<Container>::try_erase(Key k)
  requires EraseAble<Container>
{
  rw_lock_.reader_lock();

  if (unlikely(should_reject(k))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  auto erased = container_->erase(k);

  rw_lock_.reader_unlock();

  return erased;
}

template <GeneralContainerBased Container>
template <typename RetT, typename... S0s>
std::conditional_t<std::is_void_v<RetT>, bool, std::optional<RetT>>
GeneralShard<Container>::try_run(Key k, uintptr_t fn_addr, S0s... states) {
  auto fn = reinterpret_cast<RetT (*)(ContainerImpl &, Key, S0s...)>(fn_addr);
  std::size_t size = 0;

  rw_lock_.reader_lock();
  auto rw_unlocker = std::experimental::scope_exit([&] {
    if (unlikely(should_split(size))) {
      split_with_reader_lock();
    } else {
      rw_lock_.reader_unlock();
    }
  });

  if (unlikely(should_reject(k))) {
    if constexpr (std::is_void_v<RetT>) {
      return false;
    } else {
      return std::nullopt;
    }
  }

  if constexpr (std::is_void_v<RetT>) {
    size = container_->pass_through(fn, std::move(k), std::move(states)...);
    return true;
  } else {
    auto p = container_->pass_through(fn, std::move(k), std::move(states)...);
    size = p.second;
    return p.first;
  }
}

}  // namespace nu

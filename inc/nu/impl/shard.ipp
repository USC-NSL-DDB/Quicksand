#include <cmath>

#include "nu/sharding_mapping.hpp"
#include "nu/utils/caladan.hpp"
#include "nu/utils/rob_executor.hpp"

namespace nu {

template <class Container>
inline ContainerHandle<Container>::ContainerHandle(
    Container *c, GeneralShard<Container> *shard)
    : c_(c), shard_(shard) {
  shard_->rw_lock_.reader_lock();
}

template <class Container>
inline ContainerHandle<Container>::~ContainerHandle() {
  shard_->rw_lock_.reader_unlock();
}

template <class Container>
inline Container *ContainerHandle<Container>::operator->() {
  return c_;
}

template <class Container>
inline Container &ContainerHandle<Container>::operator*() {
  return *c_;
}

template <class Container>
template <class Archive>
inline void ContainerAndMetadata<Container>::save(Archive &ar) const {
  ar(capacity, container, container_bucket_size);
}

template <class Container>
template <class Archive>
inline void ContainerAndMetadata<Container>::load(Archive &ar) {
  ar(capacity);
  if constexpr (Reservable<Container>) {
    container.reserve(capacity);
  }
  ar(container, container_bucket_size);
}

template <class Container>
inline ContainerAndMetadata<Container>::ContainerAndMetadata(
    const ContainerAndMetadata &o)
    : capacity(o.capacity), container_bucket_size(o.container_bucket_size) {
  if constexpr (Reservable<Container>) {
    container.reserve(capacity);
  }
  container = o.container;
}

template <class Container>
template <class Archive>
inline void GeneralShard<Container>::ReqBatch::serialize(Archive &ar) {
  ar(l_key, r_key, shard, emplace_back_reqs, emplace_reqs);
}

template <class Container>
inline GeneralShard<Container>::GeneralShard(
    WeakProclet<ShardingMapping> mapping, uint32_t max_shard_bytes)
    : GeneralShard(mapping, max_shard_bytes, std::nullopt, std::nullopt,
                   false) {}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_bytes,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      bool reserve_space)
    : max_shard_bytes_(max_shard_bytes),
      real_max_shard_bytes_(max_shard_bytes / kAlmostFullThresh),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      deleted_(false) {
  {
    Caladan::PreemptGuard g;
    slab_ = get_runtime()->get_current_proclet_slab();
  }

  if constexpr (Reservable<Container>) {
    if (reserve_space) {
      auto old_slab_usage = slab_->get_usage();
      container_.reserve(kReserveProbeSize);
      auto new_slab_usage = slab_->get_usage();
      container_bucket_size_ =
          std::ceil(static_cast<float>(new_slab_usage - old_slab_usage) /
                    kReserveProbeSize);
      auto reserve_size =
          max_shard_bytes * kReserveContainerSizeRatio / container_bucket_size_;
      container_.reserve(reserve_size);
      initial_slab_usage_ = slab_->get_usage();
      initial_size_ = 0;
      size_thresh_ = kAlmostFullThresh * reserve_size;
    }
  }
}

template <class Container>
inline GeneralShard<Container>::~GeneralShard() {}

template <class Container>
void GeneralShard<Container>::set_range_and_data(
    std::optional<Key> l_key, std::optional<Key> r_key,
    ContainerAndMetadata<Container> container_and_metadata) {
  l_key_ = l_key;
  r_key_ = r_key;
  deleted_ = false;
  container_ = std::move(container_and_metadata.container);
  initial_slab_usage_ = slab_->get_usage();
  initial_size_ = container_.size();
  container_bucket_size_ = container_and_metadata.container_bucket_size;
  size_thresh_ = kAlmostFullThresh * container_and_metadata.capacity;
}

template <class Container>
inline Container GeneralShard<Container>::get_container_copy() {
  rw_lock_.reader_lock();

  Caladan::PreemptGuard g;
  RuntimeSlabGuard slab_guard;

  Container c = container_;
  rw_lock_.reader_unlock();

  return c;
}

template <class Container>
inline ContainerHandle<Container>
GeneralShard<Container>::get_container_handle() {
  return ContainerHandle<Container>(&container_, this);
}

template <class Container>
void GeneralShard<Container>::split() {
  Key mid_k;
  std::unique_ptr<Container> latter_half_container;
  std::size_t new_container_capacity = 0;
  auto cur_slab_usage = slab_->get_usage();

  if constexpr (Reservable<Container>) {
    auto diff_data_size = cur_slab_usage - initial_slab_usage_;
    auto diff_size = container_.size() - initial_size_;
    auto data_size = static_cast<float>(diff_data_size) / diff_size;
    new_container_capacity =
        max_shard_bytes_ / (data_size + container_bucket_size_);
  }

  BUG_ON(container_.empty());
  {
    Caladan::PreemptGuard g;
    RuntimeSlabGuard slab_guard;

    latter_half_container.reset(new Container());
    container_.split(&mid_k, latter_half_container.get());
  }

  auto new_shard = mapping_.run(&ShardingMapping::create_new_shard, mid_k,
                                r_key_, /* reserve_space = */ false);
  ContainerAndMetadata<Container> container_and_metadata;
  container_and_metadata.container = std::move(*latter_half_container);
  container_and_metadata.capacity =
      std::max(new_container_capacity, latter_half_container->size());
  container_and_metadata.container_bucket_size = container_bucket_size_;
  new_shard.run(&GeneralShard::set_range_and_data, mid_k, r_key_,
                container_and_metadata);

  r_key_ = mid_k;

  if (cur_slab_usage > real_max_shard_bytes_) {
    // Grant slightly more memory to incorporate fragmentations in our slab
    // allocator.
    real_max_shard_bytes_ = cur_slab_usage + kSlabFragmentationHeadroom;
  } else if constexpr (Reservable<Container>) {
    auto size = container_.size();
    if (size > size_thresh_) {
      size_thresh_ = size;
    }
  }
}

template <class Container>
inline bool GeneralShard<Container>::bad_range_or_deleted(
    std::optional<Key> l_key, std::optional<Key> r_key) {
  return deleted_ || (l_key < l_key_) || (r_key_ && (r_key > r_key_ || !r_key));
}

template <class Container>
inline bool GeneralShard<Container>::should_split() const {
  bool over_container_size = false;
  bool over_slab_size = false;

  if constexpr (Reservable<Container>) {
    over_container_size = (container_.size() > size_thresh_);
  }
  over_slab_size = (slab_->get_usage() > real_max_shard_bytes_);

  return over_container_size || over_slab_size;
}

template <class Container>
void GeneralShard<Container>::split_with_reader_lock() {
  rw_lock_.reader_unlock();
  rw_lock_.writer_lock();
  if (should_split()) {
    split();
  }
  rw_lock_.writer_unlock();
}

template <class Container>
void GeneralShard<Container>::delete_self_with_reader_lock() {
  rw_lock_.reader_unlock();
  if (container_.empty() && !deleted_) {
    deleted_ = mapping_.run(&ShardingMapping::delete_front_shard);
  }
}

template <class Container>
inline bool GeneralShard<Container>::try_emplace(std::optional<Key> l_key,
                                                 std::optional<Key> r_key,
                                                 DataEntry entry) {
  rw_lock_.reader_lock();

  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  if constexpr (HasVal<Container>) {
    container_.emplace(std::move(entry.first), std::move(entry.second));
  } else {
    container_.emplace(std::move(entry));
  }

  if (unlikely(should_split())) {
    split_with_reader_lock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
inline bool GeneralShard<Container>::try_emplace_back(
    std::optional<Key> l_key, std::optional<Key> r_key,
    Val v) requires EmplaceBackAble<Container> {
  rw_lock_.reader_lock();

  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.emplace_back(std::move(v));
  if (unlikely(should_split())) {
    split_with_reader_lock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
inline bool GeneralShard<Container>::try_emplace_front(
    std::optional<Key> l_key, std::optional<Key> r_key,
    Val v) requires EmplaceFrontAble<Container> {
  rw_lock_.reader_lock();
  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.push_front(std::move(v));
  if (unlikely(should_split())) {
    split_with_reader_lock();
    return true;
  }

  rw_lock_.reader_unlock();
  return true;
}

template <class Container>
inline std::optional<typename Container::Val>
GeneralShard<Container>::try_front(
    std::optional<Key> l_key,
    std::optional<Key> r_key) requires HasFront<Container> {
  rw_lock_.reader_lock();
  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  Val v = container_.front();
  rw_lock_.reader_unlock();
  return v;
}

template <class Container>
inline bool GeneralShard<Container>::try_pop_front(
    std::optional<Key> l_key,
    std::optional<Key> r_key) requires PopFrontAble<Container> {
  rw_lock_.reader_lock();

  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.pop_front();
  if (unlikely(container_.empty())) {
    delete_self_with_reader_lock();
    return true;
  }
  rw_lock_.reader_unlock();
  return true;
}

template <class Container>
inline std::optional<typename Container::Val> GeneralShard<Container>::try_back(
    std::optional<Key> l_key,
    std::optional<Key> r_key) requires HasBack<Container> {
  rw_lock_.reader_lock();
  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  Val v = container_.back();
  rw_lock_.reader_unlock();
  return v;
}

template <class Container>
inline bool GeneralShard<Container>::try_pop_back(
    std::optional<Key> l_key,
    std::optional<Key> r_key) requires PopBackAble<Container> {
  rw_lock_.reader_lock();

  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.pop_back();
  rw_lock_.reader_unlock();
  return true;
}

template <class Container>
inline std::optional<typename Container::Val>
GeneralShard<Container>::try_dequeue(
    std::optional<Key> l_key,
    std::optional<Key> r_key) requires DequeueAble<Container> {
  rw_lock_.reader_lock();

  if (unlikely(bad_range_or_deleted(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return std::nullopt;
  }

  Val v = container_.dequeue();
  if (unlikely(container_.empty())) {
    delete_self_with_reader_lock();
    return v;
  }
  rw_lock_.reader_unlock();
  return v;
}

template <class Container>
std::optional<typename GeneralShard<Container>::ReqBatch>
GeneralShard<Container>::try_handle_batch(const ReqBatch &batch) {
  rw_lock_.reader_lock();

  if (unlikely(bad_range_or_deleted(std::move(batch.l_key),
                                    std::move(batch.r_key)))) {
    rw_lock_.reader_unlock();
    return std::move(batch);
  }

  if constexpr (EmplaceBackAble<Container>) {
    if (!batch.emplace_back_reqs.empty()) {
      container_.emplace_back_batch(std::move(batch.emplace_back_reqs));
    }
  }
  if (!batch.emplace_reqs.empty()) {
    container_.emplace_batch(std::move(batch.emplace_reqs));
  }

  if (unlikely(should_split())) {
    split_with_reader_lock();
    return std::nullopt;
  }

  rw_lock_.reader_unlock();
  return std::nullopt;
}

template <class Container>
inline std::pair<bool, std::optional<typename Container::IterVal>>
GeneralShard<Container>::find_data(Key k) requires Findable<Container> {
  bool bad_range_or_deleted = (k < l_key_) || (r_key_ && k > r_key_);
  if (unlikely(bad_range_or_deleted)) {
    return std::make_pair(false, std::nullopt);
  }

  auto iter = container_.find(std::move(k));
  auto val =
      (iter != container_.cend()) ? std::make_optional(*iter) : std::nullopt;
  return std::make_pair(true, val);
}

template <class Container>
inline std::tuple<bool, typename Container::IterVal,
                  typename Container::ConstIterator>
GeneralShard<Container>::find(Key k) requires Findable<Container> {
  bool bad_range_or_deleted = (k < l_key_) || (r_key_ && k > r_key_);
  if (unlikely(bad_range_or_deleted)) {
    return std::make_tuple(false, Val(), container_.cend());
  }

  auto iter = container_.find(std::move(k));
  return std::make_tuple(true, *iter, iter);
}

template <class Container>
inline std::optional<typename Container::IterVal>
GeneralShard<Container>::find_data_by_order(
    std::size_t order) requires FindableByOrder<Container> {
  // Currently, the invocation happens only after sealing the DS. Thus we can
  // bypass all the redundant checks.
  auto iter = container_.find_by_order(order);
  if (unlikely(iter == container_.cend())) {
    return std::nullopt;
  }
  return *iter;
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_next_block_with_iters(
    ConstIterator prev_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size);
  auto size = __get_next_block_with_iters(block.begin(), prev_iter, block_size);
  block.resize(size);
  return block;
}

template <class Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_next_block(
    ConstIterator prev_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  ConstIterator end_iter;
  if (likely(container_.cend() - prev_iter > block_size + 1)) {
    end_iter = prev_iter + block_size + 1;
  } else {
    end_iter = container_.cend();
  }
  return std::vector<IterVal>(std::to_address(prev_iter + 1),
                              std::to_address(end_iter));
}

template <class Container>
uint32_t GeneralShard<Container>::__get_next_block_with_iters(
    std::vector<std::pair<IterVal, ConstIterator>>::iterator block_iter,
    ConstIterator prev_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  auto iter = prev_iter;

  uint32_t i;
  for (i = 0; i < block_size; ++i, ++block_iter) {
    if (unlikely(++iter == container_.cend())) {
      break;
    }
    *block_iter = std::pair(*iter, iter);
  }

  return i;
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_prev_block_with_iters(
    ConstIterator succ_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size);
  auto iter = succ_iter;

  uint32_t i;
  for (i = 0; i < block_size; i++) {
    if (unlikely(iter-- == container_.cbegin())) {
      break;
    }
    block[i] = std::pair(*iter, iter);
  }
  block.resize(i);

  std::reverse(block.begin(), block.end());
  return block;
}

template <class Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_prev_block(
    ConstIterator succ_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  ConstIterator begin_iter;
  if (likely(succ_iter - container_.cbegin() > block_size)) {
    begin_iter = succ_iter - block_size;
  } else {
    begin_iter = container_.cbegin();
  }
  return std::vector<IterVal>(std::to_address(begin_iter),
                              std::to_address(succ_iter));
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_next_rblock_with_iters(
    ConstReverseIterator prev_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size);
  auto size =
      __get_next_rblock_with_iters(block.begin(), prev_iter, block_size);
  block.resize(size);
  return block;
}

template <class Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_next_rblock(
    ConstReverseIterator prev_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  ConstReverseIterator end_iter;
  if (likely(container_.crend() - prev_iter > block_size + 1)) {
    end_iter = prev_iter + block_size + 1;
  } else {
    end_iter = container_.crend();
  }
  auto span =
      std::span(std::to_address(end_iter - 1), std::to_address(prev_iter));
  return std::vector<IterVal>(span.rbegin(), span.rend());
}

template <class Container>
uint32_t GeneralShard<Container>::__get_next_rblock_with_iters(
    std::vector<std::pair<IterVal, ConstReverseIterator>>::iterator block_iter,
    ConstReverseIterator prev_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  auto iter = prev_iter;

  uint32_t i;
  for (i = 0; i < block_size; ++i, ++block_iter) {
    if (unlikely(++iter == container_.crend())) {
      break;
    }
    *block_iter = std::pair(*iter, iter);
  }

  return i;
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_prev_rblock_with_iters(
    ConstReverseIterator succ_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size);
  auto iter = succ_iter;

  uint32_t i;
  for (i = 0; i < block_size; i++) {
    if (unlikely(iter-- == container_.crbegin())) {
      break;
    }
    block[i] = std::pair(*iter, iter);
  }
  block.resize(i);

  std::reverse(block.begin(), block.end());
  return block;
}

template <class Container>
std::vector<typename Container::IterVal>
GeneralShard<Container>::get_prev_rblock(
    ConstReverseIterator succ_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  ConstReverseIterator begin_iter;
  if (likely(succ_iter - container_.crbegin() > block_size)) {
    begin_iter = succ_iter - block_size;
  } else {
    begin_iter = container_.crbegin();
  }
  auto span = std::span(std::to_address(succ_iter - 1),
                        std::to_address(begin_iter - 1));
  return std::vector<IterVal>(span.rbegin(), span.rend());
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_front_block_with_iters(
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size + 1);
  block[0] = std::pair(*container_.cbegin(), container_.cbegin());
  auto size = __get_next_block_with_iters(++block.begin(), container_.cbegin(),
                                          block_size);
  block.resize(size + 1);
  return block;
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstIterator>
GeneralShard<Container>::get_front_block(
    uint32_t block_size) requires ConstIterable<Container> {
  ConstIterator end_iter;
  if (likely(container_.cend() - container_.cbegin() > block_size)) {
    end_iter = container_.cbegin() + block_size;
  } else {
    end_iter = container_.cend();
  }
  return std::make_pair(
      std::vector<IterVal>(std::to_address(container_.cbegin()),
                           std::to_address(end_iter)),
      container_.cbegin());
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rfront_block_with_iters(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size + 1);
  block[0] = std::pair(*container_.crbegin(), container_.crbegin());
  auto size = __get_next_rblock_with_iters(++block.begin(),
                                           container_.crbegin(), block_size);
  block.resize(size + 1);
  return block;
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstReverseIterator>
GeneralShard<Container>::get_rfront_block(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  ConstReverseIterator end_iter;
  if (likely(container_.crend() - container_.crbegin() > block_size)) {
    end_iter = container_.crbegin() + block_size;
  } else {
    end_iter = container_.crend();
  }
  auto span = std::span(std::to_address(end_iter - 1),
                        std::to_address(container_.crbegin() - 1));
  return std::make_pair(std::vector<IterVal>(span.rbegin(), span.rend()),
                        container_.crbegin());
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_back_block_with_iters(
    uint32_t block_size) requires ConstIterable<Container> {
  return get_prev_block_with_iters(container_.cend(), block_size);
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstIterator>
GeneralShard<Container>::get_back_block(
    uint32_t block_size) requires ConstIterable<Container> {
  auto data = get_prev_block(container_.cend(), block_size);
  return std::make_pair(std::move(data), container_.cend() - data.size());
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rback_block_with_iters(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  return get_prev_rblock_with_iters(container_.crend(), block_size);
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename Container::ConstReverseIterator>
GeneralShard<Container>::get_rback_block(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  auto data = get_prev_rblock(container_.crend(), block_size);
  return std::make_pair(std::move(data), container_.crend() - data.size());
}

template <class Container>
inline typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cbegin() requires ConstIterable<Container> {
  return container_.cbegin();
}

template <class Container>
inline typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::clast() requires ConstIterable<Container> {
  return --container_.cend();
}

template <class Container>
inline typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cend() requires ConstIterable<Container> {
  return container_.cend();
}

template <class Container>
inline typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crbegin() requires ConstReverseIterable<Container> {
  return container_.crbegin();
}

template <class Container>
inline typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crlast() requires ConstReverseIterable<Container> {
  return --container_.crend();
}

template <class Container>
inline typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crend() requires ConstReverseIterable<Container> {
  return container_.crend();
}

template <class Container>
inline bool GeneralShard<Container>::empty() {
  return container_.empty();
}

template <class Container>
inline std::size_t GeneralShard<Container>::size() {
  return container_.size();
}

template <class Container>
inline Container::Key GeneralShard<
    Container>::split_at_end() requires GeneralContainer::kContiguousIterator {
  if (r_key_) {
    return *r_key_;
  }

  r_key_ = l_key_.value_or(0) + container_.size();
  return *r_key_;
}

template <class Container>
inline Container::Key GeneralShard<Container>::rebase(
    Key new_l_key) requires GeneralContainer::kContiguousIterator {
  l_key_ = new_l_key;
  auto new_r_key = container_.rebase(new_l_key);
  if (r_key_) {
    r_key_ = new_r_key;
  }
  return new_r_key;
}

}  // namespace nu

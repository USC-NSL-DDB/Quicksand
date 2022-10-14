#include "nu/sharding_mapping.hpp"
#include "nu/utils/rob_executor.hpp"

namespace nu {

template <class Container>
ContainerHandle<Container>::ContainerHandle(Container *c,
                                            GeneralShard<Container> *shard)
    : c_(c), shard_(shard) {
  shard_->rw_lock_.writer_lock();
}

template <class Container>
ContainerHandle<Container>::~ContainerHandle() {
  shard_->rw_lock_.writer_unlock();
}

template <class Container>
Container *ContainerHandle<Container>::operator->() {
  return c_;
}

template <class Container>
Container &ContainerHandle<Container>::operator*() {
  return *c_;
}

template <class Container>
ConstContainerHandle<Container>::ConstContainerHandle(
    const Container *c, GeneralShard<Container> *shard)
    : c_(c), shard_(shard) {
  shard_->rw_lock_.writer_lock();
}

template <class Container>
ConstContainerHandle<Container>::~ConstContainerHandle() {
  shard_->rw_lock_.writer_unlock();
}

template <class Container>
const Container *ConstContainerHandle<Container>::operator->() {
  return c_;
}

template <class Container>
const Container &ConstContainerHandle<Container>::operator*() {
  return *c_;
}

template <class Container>
template <class Archive>
void ContainerWithCapacity<Container>::save(Archive &ar) const {
  ar(capacity, container);
}

template <class Container>
template <class Archive>
void ContainerWithCapacity<Container>::load(Archive &ar) {
  ar(capacity);
  if constexpr (Reservable<Container>) {
    container.reserve(capacity);
  }
  ar(container);
}

template <class Container>
template <class Archive>
void GeneralShard<Container>::ReqBatch::serialize(Archive &ar) {
  ar(l_key, r_key, shard, emplace_back_reqs, emplace_reqs);
}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_bytes)
    : GeneralShard(mapping, max_shard_bytes, std::nullopt, std::nullopt, 0) {}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_bytes,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      std::size_t capacity)
    : max_shard_bytes_(max_shard_bytes),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      container_(capacity),
      slab_(Runtime::get_current_proclet_slab()) {}

template <class Container>
GeneralShard<Container>::~GeneralShard() {
  // flush all reader or writer handles
  rw_lock_.writer_lock();
  rw_lock_.writer_unlock();
}

template <class Container>
void GeneralShard<Container>::set_range_and_data(
    std::optional<Key> l_key, std::optional<Key> r_key,
    ContainerWithCapacity<Container> container_with_capacity) {
  l_key_ = l_key;
  r_key_ = r_key;
  container_ = std::move(container_with_capacity.container);
}

template <class Container>
Container GeneralShard<Container>::get_container_copy() {
  // FIXME: be migration-safe.
  RuntimeSlabGuard slab_guard;

  rw_lock_.reader_lock();
  Container c = container_;
  rw_lock_.reader_unlock();
  return c;
}

template <class Container>
ContainerHandle<Container> GeneralShard<Container>::get_container_handle() {
  return ContainerHandle<Container>(&container_, this);
}

template <class Container>
ConstContainerHandle<Container>
GeneralShard<Container>::get_const_container_handle() {
  return ConstContainerHandle<Container>(&container_, this);
}

template <class Container>
void GeneralShard<Container>::split() {
  // FIXME: be migration-safe.
  RuntimeSlabGuard slab_guard;

  BUG_ON(container_.empty());
  auto [mid_k, latter_half_container] = container_.split();
  auto new_shard =
      mapping_.run(&ShardingMapping::create_new_shard, mid_k, r_key_, 0);
  ContainerWithCapacity container_with_capacity{
      std::move(latter_half_container), container_.size()};
  new_shard.run(&GeneralShard::set_range_and_data, mid_k, r_key_,
                container_with_capacity);
  r_key_ = mid_k;
  // Grant slightly more memory to incorporate fragmentations in our slab
  // allocator.
  max_shard_bytes_ = static_cast<uint32_t>(slab_->get_usage()) + (2 << 20);
}

template <class Container>
bool GeneralShard<Container>::bad_range(std::optional<Key> l_key,
                                        std::optional<Key> r_key) {
  return (l_key < l_key_) || (r_key_ && (r_key > r_key_ || !r_key));
}

template <class Container>
bool GeneralShard<Container>::should_split() const {
  return slab_->get_usage() > max_shard_bytes_;
}

template <class Container>
bool GeneralShard<Container>::try_emplace(std::optional<Key> l_key,
                                          std::optional<Key> r_key, Pair p) {
  rw_lock_.reader_lock();

  if (unlikely(bad_range(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.emplace(std::move(p.first), std::move(p.second));

  if (unlikely(should_split())) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (should_split()) {
      split();
    }
    rw_lock_.writer_unlock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
bool GeneralShard<Container>::try_emplace_back(
    std::optional<Key> l_key, std::optional<Key> r_key,
    Val v) requires EmplaceBackAble<Container> {
  rw_lock_.reader_lock();

  if (unlikely(bad_range(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.emplace_back(std::move(v));
  if (unlikely(should_split())) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (should_split()) {
      split();
    }
    rw_lock_.writer_unlock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
std::optional<typename GeneralShard<Container>::ReqBatch>
GeneralShard<Container>::__try_handle_batch(const ReqBatch &batch) {
  rw_lock_.reader_lock();

  if (unlikely(bad_range(std::move(batch.l_key), std::move(batch.r_key)))) {
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
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (should_split()) {
      split();
    }
    rw_lock_.writer_unlock();
    return std::nullopt;
  }

  rw_lock_.reader_unlock();
  return std::nullopt;
}

template <class Container>
uintptr_t GeneralShard<Container>::new_flush_executor(uint32_t queue_depth) {
  auto *rob_executor = new RobExecutor<ReqBatch, std::optional<ReqBatch>>(
      [&](const ReqBatch &batch) { return __try_handle_batch(batch); },
      queue_depth);
  auto addr = reinterpret_cast<uintptr_t>(rob_executor);
  return addr;
}

template <class Container>
void GeneralShard<Container>::delete_flush_executor(uintptr_t addr) {
  auto *rob_executor =
      reinterpret_cast<RobExecutor<ReqBatch, std::optional<ReqBatch>> *>(addr);
  delete rob_executor;
}

template <class Container>
std::vector<typename GeneralShard<Container>::ReqBatch>
GeneralShard<Container>::try_handle_batch(ReqBatch batch, uint32_t seq,
                                          uintptr_t rob_executor_addr,
                                          bool drain) {
  std::vector<ReqBatch> rejected_batches;
  auto *rob_executor =
      reinterpret_cast<RobExecutor<ReqBatch, std::optional<ReqBatch>> *>(
          rob_executor_addr);

  auto optional_batch = rob_executor->submit_and_get(seq, std::move(batch));
  if (optional_batch && *optional_batch) {
    rejected_batches.emplace_back(std::move(**optional_batch));
  }

  if (drain) {
    auto optional_batches = rob_executor->wait_all(seq + 1);
    for (auto &optional_batch : optional_batches) {
      if (optional_batch) {
        rejected_batches.emplace_back(std::move(*optional_batch));
      }
    }
  }

  return rejected_batches;
}

template <class Container>
std::pair<bool, std::optional<typename Container::IterVal>>
GeneralShard<Container>::find_val(Key k) requires Findable<Container> {
  bool bad_range = (k < l_key_) || (r_key_ && k > r_key_);
  if (unlikely(bad_range)) {
    return std::make_pair(false, std::nullopt);
  }

  auto iter = container_.find(std::move(k));
  auto val =
      (iter != container_.cend()) ? std::make_optional(*iter) : std::nullopt;
  return std::make_pair(true, val);
}

template <class Container>
std::tuple<bool, typename Container::Val, typename Container::ConstIterator>
GeneralShard<Container>::find(Key k) requires Findable<Container> {
  bool bad_range = (k < l_key_) || (r_key_ && k > r_key_);
  if (unlikely(bad_range)) {
    return std::make_tuple(false, Val(), container_.cend());
  }

  auto iter = container_.find(std::move(k));
  return std::make_tuple(true, *iter, iter);
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_block_forward(
    ConstIterator prev_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size);
  auto size = __get_block_forward(block.begin(), prev_iter, block_size);
  block.resize(size);
  return block;
}

template <class Container>
uint32_t GeneralShard<Container>::__get_block_forward(
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
GeneralShard<Container>::get_block_backward(
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
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rblock_forward(
    ConstReverseIterator prev_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size);
  auto size = __get_rblock_forward(block.begin(), prev_iter, block_size);
  block.resize(size);
  return block;
}

template <class Container>
uint32_t GeneralShard<Container>::__get_rblock_forward(
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
GeneralShard<Container>::get_rblock_backward(
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
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_front_block(
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.resize(block_size + 1);
  block[0] = std::pair(*container_.cbegin(), container_.cbegin());
  auto size =
      __get_block_forward(++block.begin(), container_.cbegin(), block_size);
  block.resize(size + 1);
  return block;
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rfront_block(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.resize(block_size + 1);
  block[0] = std::pair(*container_.crbegin(), container_.crbegin());
  auto size =
      __get_rblock_forward(++block.begin(), container_.crbegin(), block_size);
  block.resize(size + 1);
  return block;
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_back_block(
    uint32_t block_size) requires ConstIterable<Container> {
  return get_block_backward(container_.cend(), block_size);
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rback_block(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  return get_rblock_backward(container_.crend(), block_size);
}

template <class Container>
typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cbegin() requires ConstIterable<Container> {
  return container_.cbegin();
}

template <class Container>
typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::clast() requires ConstIterable<Container> {
  return --container_.cend();
}

template <class Container>
typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cend() requires ConstIterable<Container> {
  return container_.cend();
}

template <class Container>
typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crbegin() requires ConstReverseIterable<Container> {
  return container_.crbegin();
}

template <class Container>
typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crlast() requires ConstReverseIterable<Container> {
  return --container_.crend();
}

template <class Container>
typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crend() requires ConstReverseIterable<Container> {
  return container_.crend();
}

template <class Container>
bool GeneralShard<Container>::empty() {
  return container_.empty();
}
}  // namespace nu

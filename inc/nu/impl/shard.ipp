#include "nu/sharding_mapping.hpp"

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

template <typename Pair>
inline constexpr uint64_t get_proclet_capacity(uint32_t max_shard_size) {
  return 8 * max_shard_size * sizeof(Pair);
}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_size,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      Container container)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      container_(std::move(container)) {}

template <class Container>
GeneralShard<Container>::GeneralShard(WeakProclet<ShardingMapping> mapping,
                                      uint32_t max_shard_size,
                                      std::optional<Key> l_key,
                                      std::optional<Key> r_key,
                                      std::size_t capacity)
    : max_shard_size_(max_shard_size),
      mapping_(std::move(mapping)),
      l_key_(l_key),
      r_key_(r_key),
      container_(l_key, capacity) {}

template <class Container>
GeneralShard<Container>::~GeneralShard() {
  // flush all reader or writer handles
  rw_lock_.writer_lock();
  rw_lock_.writer_unlock();
}

template <class Container>
Container GeneralShard<Container>::get_container_copy() {
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
  auto [mid_k, latter_half_container] = container_.split();
  auto proclet_capacity = get_proclet_capacity<Pair>(max_shard_size_);
  auto new_shard = make_proclet_with_capacity<GeneralShard>(
      proclet_capacity, mapping_, max_shard_size_, mid_k, r_key_,
      latter_half_container);
  r_key_ = mid_k;
  mapping_.run(&ShardingMapping::update_mapping, mid_k, std::move(new_shard));
}

template <class Container>
bool GeneralShard<Container>::bad_range(std::optional<Key> l_key,
                                        std::optional<Key> r_key) {
  return (l_key < l_key_) || (r_key_ && (r_key > r_key_ || !r_key));
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
  if (unlikely(container_.size() > max_shard_size_)) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (container_.size() > max_shard_size_) {
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
  if (unlikely(container_.size() > max_shard_size_)) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (container_.size() > max_shard_size_) {
      split();
    }
    rw_lock_.writer_unlock();
    return true;
  }

  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
bool GeneralShard<Container>::try_handle_batch(
    std::optional<Key> l_key, std::optional<Key> r_key,
    std::vector<ContainerReq<Key, Val>> reqs) {
  rw_lock_.reader_lock();

  if (unlikely(bad_range(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  container_.handle_batch(std::move(reqs));
  if (unlikely(container_.size() > max_shard_size_)) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (container_.size() > max_shard_size_) {
      split();
    }
    rw_lock_.writer_unlock();
    return true;
  }

  rw_lock_.reader_unlock();
  return true;
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
  block.reserve(block_size);
  __get_block_forward(&block, prev_iter, block_size);
  return block;
}

template <class Container>
void GeneralShard<Container>::__get_block_forward(
    std::vector<std::pair<IterVal, ConstIterator>> *block,
    ConstIterator prev_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  auto iter = prev_iter;

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(++iter == container_.cend())) {
      break;
    }
    block->emplace_back(*iter, iter);
  }
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_block_backward(
    ConstIterator succ_iter,
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.reserve(block_size);
  auto iter = succ_iter;

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(iter-- == container_.cbegin())) {
      break;
    }
    block.emplace_back(*iter, iter);
  }
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
  block.reserve(block_size);
  __get_rblock_forward(&block, prev_iter, block_size);
  return block;
}

template <class Container>
void GeneralShard<Container>::__get_rblock_forward(
    std::vector<std::pair<IterVal, ConstReverseIterator>> *block,
    ConstReverseIterator prev_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  auto iter = prev_iter;

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(++iter == container_.crend())) {
      break;
    }
    block->emplace_back(*iter, iter);
  }
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rblock_backward(
    ConstReverseIterator succ_iter,
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.reserve(block_size);
  auto iter = succ_iter;

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(iter-- == container_.crbegin())) {
      break;
    }
    block.emplace_back(*iter, iter);
  }
  std::reverse(block.begin(), block.end());
  return block;
}

template <class Container>
std::vector<
    std::pair<typename Container::IterVal, typename Container::ConstIterator>>
GeneralShard<Container>::get_front_block(
    uint32_t block_size) requires ConstIterable<Container> {
  std::vector<std::pair<IterVal, ConstIterator>> block;
  block.reserve(block_size);
  block.emplace_back(*container_.cbegin(), container_.cbegin());
  __get_block_forward(&block, container_.cbegin(), block_size);
  return block;
}

template <class Container>
std::vector<std::pair<typename Container::IterVal,
                      typename Container::ConstReverseIterator>>
GeneralShard<Container>::get_rfront_block(
    uint32_t block_size) requires ConstReverseIterable<Container> {
  std::vector<std::pair<IterVal, ConstReverseIterator>> block;
  block.reserve(block_size);
  block.emplace_back(*container_.crbegin(), container_.crbegin());
  __get_rblock_forward(&block, container_.crbegin(), block_size);
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

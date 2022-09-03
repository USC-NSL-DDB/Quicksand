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
  return 4 * max_shard_size * sizeof(Pair);
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
      container_(capacity) {}

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

  if (unlikely(container_.size() >= max_shard_size_)) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (container_.size() >= max_shard_size_) {
      split();
    }
    rw_lock_.writer_unlock();
    return false;
  }

  container_.emplace(std::move(p.first), std::move(p.second));
  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
bool GeneralShard<Container>::try_emplace_back(std::optional<Key> l_key,
                                               std::optional<Key> r_key,
                                               Val v) {
  rw_lock_.reader_lock();

  if (unlikely(bad_range(std::move(l_key), std::move(r_key)))) {
    rw_lock_.reader_unlock();
    return false;
  }

  if (unlikely(container_.size() >= max_shard_size_)) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (container_.size() >= max_shard_size_) {
      split();
    }
    rw_lock_.writer_unlock();
    return false;
  }

  container_.emplace_back(std::move(v));
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

  // make the most pessimistic estimation for now. Can be more intelligent.
  if (unlikely(container_.size() + reqs.size() > max_shard_size_)) {
    rw_lock_.reader_unlock();
    rw_lock_.writer_lock();
    if (container_.size() + reqs.size() > max_shard_size_) {
      split();
    }
    rw_lock_.writer_unlock();
    return false;
  }

  container_.handle_batch(std::move(reqs));
  rw_lock_.reader_unlock();

  return true;
}

template <class Container>
std::pair<bool, std::optional<typename GeneralShard<Container>::Val>>
GeneralShard<Container>::find_val(Key k) {
  bool bad_range = (k < l_key_) || (r_key_ && k > r_key_);
  if (unlikely(bad_range)) {
    return std::make_pair(false, std::nullopt);
  }

  return std::make_pair(true, container_.find_val(std::move(k)));
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename GeneralShard<Container>::ConstIterator>
GeneralShard<Container>::get_block_forward(ConstIterator start_iter,
                                           uint32_t block_size) {
  std::vector<IterVal> vals;
  auto iter = start_iter;

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(iter == container_.cend())) {
      break;
    }
    vals.push_back(*iter);
    iter++;
  }
  return std::make_pair(std::move(vals), std::move(iter));
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename GeneralShard<Container>::ConstIterator>
GeneralShard<Container>::get_block_backward(ConstIterator end_iter,
                                            uint32_t block_size) {
  std::vector<IterVal> vals;
  auto iter = end_iter;

  if (unlikely(end_iter == container_.cend())) {
    auto mod = container_.size() % block_size;
    block_size = mod ? mod : block_size;
  }

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(iter == container_.cbegin())) {
      break;
    }
    iter--;
    vals.push_back(*iter);
  }
  return std::make_pair(std::move(vals), std::move(iter));
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename GeneralShard<Container>::ConstReverseIterator>
GeneralShard<Container>::get_rblock_forward(ConstReverseIterator start_iter,
                                            uint32_t block_size) {
  std::vector<IterVal> vals;
  auto iter = start_iter;

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(iter == container_.crend())) {
      break;
    }
    vals.push_back(*iter);
    iter++;
  }
  return std::make_pair(std::move(vals), std::move(iter));
}

template <class Container>
std::pair<std::vector<typename Container::IterVal>,
          typename GeneralShard<Container>::ConstReverseIterator>
GeneralShard<Container>::get_rblock_backward(ConstReverseIterator end_iter,
                                             uint32_t block_size) {
  std::vector<IterVal> vals;
  auto iter = end_iter;

  if (unlikely(end_iter == container_.crend())) {
    auto mod = container_.size() % block_size;
    block_size = mod ? mod : block_size;
  }

  for (uint32_t i = 0; i < block_size; i++) {
    if (unlikely(iter == container_.crbegin())) {
      break;
    }
    iter--;
    vals.push_back(*iter);
  }
  return std::make_pair(std::move(vals), std::move(iter));
}

template <class Container>
typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cbegin() {
  return container_.cbegin();
}

template <class Container>
typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::clast() {
  return --container_.cend();
}

template <class Container>
typename GeneralShard<Container>::ConstIterator
GeneralShard<Container>::cend() {
  return container_.cend();
}

template <class Container>
typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crbegin() {
  return container_.crbegin();
}

template <class Container>
typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crlast() {
  return --container_.crend();
}

template <class Container>
typename GeneralShard<Container>::ConstReverseIterator
GeneralShard<Container>::crend() {
  return container_.crend();
}

template <class Container>
bool GeneralShard<Container>::empty() {
  return container_.empty();
}

}  // namespace nu

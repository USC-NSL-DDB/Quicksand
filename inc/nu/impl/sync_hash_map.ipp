namespace nu {

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::SyncHashMap() {
  for (size_t i = 0; i < NBuckets; i++) {
    buckets_[i].pair = buckets_[i].next = nullptr;
  }
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1>
std::optional<V>
SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::get(K1 &&k) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  return get_with_hash(std::forward<K1>(k), key_hash);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1>
std::optional<V>
SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::get_with_hash(
    K1 &&k, uint64_t key_hash) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  auto &lock = locks_[bucket_idx];
  lock.Lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        auto ret = std::make_optional(pair->second);
        lock.Unlock();
        return ret;
      }
    }
    bucket_node = bucket_node->next;
  }
  lock.Unlock();
  return std::nullopt;
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename V1>
void SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::put(K1 &&k,
                                                                       V1 &&v) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  put_with_hash(std::forward<K1>(k), std::forward<V1>(v), key_hash);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename V1>
void SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::put_with_hash(K1 &&k, V1 &&v, uint64_t key_hash) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto &lock = locks_[bucket_idx];
  lock.Lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        pair->second = std::forward<V1>(v);
        lock.Unlock();
        return;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }

  auto allocator = Allocator();
  auto *pair = allocator.allocate(1);
  new (pair) Pair(std::forward<K1>(k), std::forward<V1>(v));

  if (!prev_next) {
    bucket_node->key_hash = key_hash;
    bucket_node->pair = pair;
  } else {
    BucketNodeAllocator bucket_node_allocator;
    auto *new_bucket_node = bucket_node_allocator.allocate(1);
    new (new_bucket_node) BucketNode();
    new_bucket_node->key_hash = key_hash;
    new_bucket_node->pair = pair;
    new_bucket_node->next = nullptr;
    *prev_next = new_bucket_node;
  }
  lock.Unlock();
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1>
bool SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::remove(
    K1 &&k) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  return remove_with_hash(std::forward<K1>(k), key_hash);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1>
bool SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::remove_with_hash(K1 &&k, uint64_t key_hash) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto &lock = locks_[bucket_idx];
  lock.Lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        if (!prev_next) {
          if (!bucket_node->next) {
            bucket_node->pair = nullptr;
          } else {
            *bucket_node = *bucket_node->next;
          }
        } else {
          *prev_next = bucket_node->next;
        }
        lock.Unlock();
        auto allocator = Allocator();
        std::destroy_at(pair);
        allocator.deallocate(pair, 1);
        if (prev_next) {
          BucketNodeAllocator bucket_node_allocator;
          bucket_node_allocator.deallocate(bucket_node, 1);
        }
        return true;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }
  lock.Unlock();
  return false;
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename RetT, typename... A0s, typename... A1s>
RetT SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::apply(
    K1 &&k, RetT (*fn)(std::pair<const K, V> &, A0s...), A1s &&... args) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  return apply_with_hash(std::forward<K1>(k), key_hash, fn,
                         std::forward<A1s>(args)...);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename RetT, typename... A0s, typename... A1s>
RetT SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::apply_with_hash(K1 &&k, uint64_t key_hash,
                                        RetT (*fn)(std::pair<const K, V> &,
                                                   A0s...),
                                        A1s &&... args) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  BucketNode **prev_next = nullptr;

  Pair *pair;
  Allocator allocator;

  auto &lock = locks_[bucket_idx];
  lock.Lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        goto apply_fn;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }

  pair = allocator.allocate(1);
  new (pair) Pair(std::forward<K1>(k), V());

  if (!prev_next) {
    bucket_node->key_hash = key_hash;
    bucket_node->pair = pair;
  } else {
    BucketNodeAllocator bucket_node_allocator;
    auto *new_bucket_node = bucket_node_allocator.allocate(1);
    new (new_bucket_node) BucketNode();
    new_bucket_node->key_hash = key_hash;
    new_bucket_node->pair = pair;
    new_bucket_node->next = nullptr;
    *prev_next = new_bucket_node;
  }

apply_fn:
  if constexpr (!std::is_same<RetT, void>::value) {
    auto ret = fn(*pair, std::forward<A1s>(args)...);
    lock.Unlock();
    return ret;
  } else {
    fn(*pair, std::forward<A1s>(args)...);
    lock.Unlock();
  }
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename RetT, typename... A0s, typename... A1s>
RetT SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::associative_reduce(RetT init_val,
                                           void (*reduce_fn)(
                                               RetT &,
                                               const std::pair<const K, V> &,
                                               A0s...),
                                           A1s &&... args) {
  RetT reduced_val(std::move(init_val));
  for (size_t i = 0; i < NBuckets; i++) {
    if (buckets_[i].pair) {
      locks_[i].Lock();
      auto *bucket_node = &buckets_[i];
      while (bucket_node && bucket_node->pair) {
        auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
        reduce_fn(reduced_val, *pair, std::forward<A1s>(args)...);
        bucket_node = bucket_node->next;
      }
      locks_[i].Unlock();
    }
  }
  return reduced_val;
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
std::vector<std::pair<K, V>>
SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::get_all_pairs() {
  return associative_reduce(
      std::vector<std::pair<K, V>>(),
      +[](std::vector<std::pair<K, V>> &reduced_val,
          const std::pair<const K, V> &pair) { reduced_val.push_back(pair); });
}

} // namespace nu

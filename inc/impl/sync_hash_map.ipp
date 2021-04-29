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
  auto key_hash = hasher(std::forward<K1>(k));
  return get_with_hash(k, key_hash);
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
      auto pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(std::forward<K1>(k), pair->first)) {
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
  auto key_hash = hasher(std::forward<K1>(k));
  put_with_hash(k, v, key_hash);
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
      auto pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(std::forward<K1>(k), pair->first)) {
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
  auto key_hash = hasher(std::forward<K1>(k));
  return remove_with_hash(k, key_hash);
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
      auto pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(std::forward<K1>(k), pair->first)) {
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
        return true;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }
  lock.Unlock();
  return false;
}

} // namespace nu

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
V *SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::get(K1 &&k) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  return get_with_hash(std::forward<K1>(k), key_hash);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1>
V *SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::get_with_hash(
    K1 &&k, uint64_t key_hash) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  auto &lock = locks_[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        auto ret = &pair->second;
        lock.unlock();
        return ret;
      }
    }
    bucket_node = bucket_node->next;
  }
  lock.unlock();
  return nullptr;
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename V1>
void SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::put(K1 k,
                                                                       V1 v) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  put_with_hash(std::move(k), std::move(v), key_hash);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename V1>
void SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::put_with_hash(K1 k, V1 v, uint64_t key_hash) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto &lock = locks_[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        pair->second = std::forward<V1>(v);
        lock.unlock();
        return;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }

  auto allocator = Allocator();
  auto *pair = allocator.allocate(1);
  new (pair) Pair(k, v);

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
  lock.unlock();
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename... Args>
bool SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::try_emplace(
    K1 k, Args... args) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  return try_emplace_with_hash(k, key_hash, std::move(args)...);
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename K1, typename... Args>
bool SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::try_emplace_with_hash(K1 k, uint64_t key_hash,
                                              Args... args) {
  auto equaler = KeyEqual();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &buckets_[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto &lock = locks_[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        lock.unlock();
        return false;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }

  auto allocator = Allocator();
  auto *pair = allocator.allocate(1);
  new (pair) Pair(k, V1(args)...);

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
  lock.unlock();
  return true;
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
  BucketNodeAllocator bucket_node_allocator;
  auto &lock = locks_[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        if (!prev_next) {
          if (!bucket_node->next) {
            bucket_node->pair = nullptr;
          } else {
            auto *next = bucket_node->next;
            *bucket_node = *next;
            bucket_node_allocator.deallocate(next, 1);
          }
        } else {
          *prev_next = bucket_node->next;
          bucket_node_allocator.deallocate(bucket_node, 1);
        }
        lock.unlock();
        auto allocator = Allocator();
        std::destroy_at(pair);
        allocator.deallocate(pair, 1);
        return true;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }
  lock.unlock();
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
  lock.lock();

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
    lock.unlock();
    return ret;
  } else {
    fn(*pair, std::forward<A1s>(args)...);
    lock.unlock();
  }
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
template <typename RetT, typename... A0s, typename... A1s>
RetT SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator,
                 Lock>::associative_reduce(bool clear, RetT init_val,
                                           void (*reduce_fn)(
                                               RetT &, std::pair<const K, V> &,
                                               A0s...),
                                           A1s &&... args) {
  Allocator allocator;
  BucketNodeAllocator bucket_node_allocator;

  RetT reduced_val(std::move(init_val));
  for (size_t i = 0; i < NBuckets; i++) {
    if (buckets_[i].pair) {
      locks_[i].lock();
      auto *bucket_node = &buckets_[i];
      bool head = true;
      while (bucket_node && bucket_node->pair) {
        auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
        reduce_fn(reduced_val, *pair, std::forward<A1s>(args)...);
        auto *next = bucket_node->next;

        if (clear) {
          std::destroy_at(pair);
          allocator.deallocate(pair, 1);
	  if (head) {
	    head = false;
	    bucket_node->pair = bucket_node->next = nullptr;
          } else {
            bucket_node_allocator.deallocate(bucket_node, 1);
          }
        }

        bucket_node = next;
      }
      locks_[i].unlock();
    }
  }
  return reduced_val;
}

template <size_t NBuckets, typename K, typename V, typename Hash,
          typename KeyEqual, typename Allocator, typename Lock>
std::vector<std::pair<K, V>>
SyncHashMap<NBuckets, K, V, Hash, KeyEqual, Allocator, Lock>::get_all_pairs() {
  return associative_reduce(
      /* clear = */ false, /* init_val = */ std::vector<std::pair<K, V>>(),
      /* reduce_fn = */
      +[](std::vector<std::pair<K, V>> &reduced_val,
          std::pair<const K, V> &pair) { reduced_val.push_back(pair); });
}

} // namespace nu

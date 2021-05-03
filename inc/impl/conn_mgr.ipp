#include <memory>

extern "C" {
#include <runtime/tcp.h>
}

namespace nu {

// TODO: refactor this into two classes.

template <typename Key>
ConnectionManager<Key>::ConnectionManager(
    const std::function<tcpconn_t *(Key)> &creator,
    uint32_t per_core_cache_size)
    : creator_(creator), per_core_cache_size_(per_core_cache_size) {}

template <typename Key>
ConnectionManager<Key>::ConnectionManager(
    std::function<tcpconn_t *(Key)> &&creator, uint32_t per_core_cache_size)
    : creator_(std::move(creator)), per_core_cache_size_(per_core_cache_size) {}

template <typename Key> ConnectionManager<Key>::~ConnectionManager() {}

template <typename Key> tcpconn_t *ConnectionManager<Key>::get_conn(Key k) {
retry:
  int cpu = get_cpu();
  tcpconn_t *conn = nullptr;
  auto &cached_conns = cached_conns_[cpu][k];
  if (!cached_conns.empty()) {
    conn = cached_conns.top();
    cached_conns.pop();
  }

  if (unlikely(!conn)) {
    global_spin_.Lock();
    auto &global = global_conns_[k];
    auto expected_size =
        std::max(static_cast<uint32_t>(1), per_core_cache_size_);
    while (!global.empty() && cached_conns.size() < expected_size) {
      cached_conns.push(global.top());
      global.pop();
    }
    if (unlikely(cached_conns.size() < expected_size)) {
      global_spin_.Unlock();
      put_cpu();
      reserve_conns(k, expected_size);
      goto retry;
    }
    global_spin_.Unlock();
    conn = cached_conns.top();
    cached_conns.pop();
  }
  put_cpu();

  return conn;
}

template <typename Key>
void ConnectionManager<Key>::put_conn(Key k, tcpconn_t *conn) {
  int cpu = get_cpu();
  auto &cached_conns = cached_conns_[cpu][k];
  cached_conns.push(conn);

  if (unlikely(cached_conns.size() > per_core_cache_size_)) {
    rt::ScopedLock<rt::Spin> lock(&global_spin_);
    auto &global = global_conns_[k];
    while (cached_conns.size() > (per_core_cache_size_ + 1) / 2) {
      global.push(cached_conns.top());
      cached_conns.pop();
    }
  }
  put_cpu();
}

template <typename Key>
void ConnectionManager<Key>::reserve_conns(Key k, uint32_t num) {
  auto conns = std::make_unique<tcpconn_t *[]>(num);
  for (uint32_t i = 0; i < num; i++) {
    conns[i] = creator_(k);
  }

  rt::ScopedLock<rt::Spin> lock(&global_spin_);
  auto &global = global_conns_[k];
  for (uint32_t i = 0; i < num; i++) {
    global.push(conns[i]);
  }
}

} // namespace nu

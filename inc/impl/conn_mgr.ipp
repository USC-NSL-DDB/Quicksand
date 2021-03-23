extern "C" {
#include <runtime/preempt.h>
#include <runtime/tcp.h>
}

namespace nu {

template <typename Key>
ConnectionManager<Key>::ConnectionManager(
    const std::function<tcpconn_t *(Key)> &creator)
    : creator_(creator) {}

template <typename Key>
ConnectionManager<Key>::ConnectionManager(
    std::function<tcpconn_t *(Key)> &&creator)
    : creator_(std::move(creator)) {}

template <typename Key> ConnectionManager<Key>::~ConnectionManager() {
  for (size_t i = 0; i < kNumCores; i++) {
    for (auto &[_, stack] : cached_conns_[i]) {
      while (!stack.empty()) {
	auto conn = stack.top();
        stack.pop();
        if (conn) {
          tcp_abort(conn);
          tcp_close(conn);
        }
      }
    }
  }
}

template <typename Key> tcpconn_t *ConnectionManager<Key>::get_conn(Key k) {
  int cpu = get_cpu();
  tcpconn_t *conn;
  auto &conns = cached_conns_[cpu][k];
  if (conns.empty()) {
    conn = nullptr;
  } else {
    conn = conns.top();
    conns.pop();
  }
  put_cpu();

  if (!conn) {
    conn = creator_(k);
  }
  return conn;
}

template <typename Key>
void ConnectionManager<Key>::put_conn(Key k, tcpconn_t *conn) {
  int cpu = get_cpu();
  cached_conns_[cpu][k].push(conn);
  put_cpu();
}

} // namespace nu

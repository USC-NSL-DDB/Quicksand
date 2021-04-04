#pragma once

#include <functional>
#include <stack>
#include <unordered_map>
#include <utility>

extern "C" {
#include <runtime/tcp.h>
}

#include "defs.hpp"
#include "runtime_alloc.hpp"

namespace nu {

template <typename Key> class ConnectionManager {
public:
  ConnectionManager(const std::function<tcpconn_t *(Key)> &creator);
  ConnectionManager(std::function<tcpconn_t *(Key)> &&creator);
  ~ConnectionManager();
  tcpconn_t *get_conn(Key k);
  void put_conn(Key k, tcpconn_t *conn);

private:
  using Val =
      std::stack<tcpconn_t *,
                 std::deque<tcpconn_t *, RuntimeAllocator<tcpconn_t *>>>;
  using Hash = std::hash<Key>;
  using KeyEqual = std::equal_to<Key>;
  using Allocator = RuntimeAllocator<std::pair<const Key, Val>>;
  std::unordered_map<Key, Val, Hash, KeyEqual, Allocator>
      cached_conns_[kNumCores];
  std::function<tcpconn_t *(Key)> creator_;
};
} // namespace nu

#include "impl/conn_mgr.ipp"

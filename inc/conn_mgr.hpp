#pragma once

#include <functional>
#include <stack>
#include <unordered_map>
#include <utility>

extern "C" {
#include <runtime/tcp.h>
}
#include <sync.h>

#include "defs.hpp"
#include "runtime_alloc.hpp"
#include "utils/cached_pool.hpp"

namespace nu {

template <typename Key> class ConnectionManager {
public:
  ConnectionManager(const std::function<tcpconn_t *(Key)> &creator,
                    uint32_t per_core_cache_size);
  ConnectionManager(std::function<tcpconn_t *(Key)> &&creator,
                    uint32_t per_core_cache_size);
  ~ConnectionManager();
  tcpconn_t *get_conn(Key k);
  void put_conn(Key k, tcpconn_t *conn);
  void reserve_conns(Key k, uint32_t num);

private:
  using Val =
      std::stack<tcpconn_t *,
                 std::vector<tcpconn_t *, RuntimeAllocator<tcpconn_t *>>>;
  using Hash = std::hash<Key>;
  using KeyEqual = std::equal_to<Key>;
  using Allocator = RuntimeAllocator<std::pair<const Key, Val>>;
  std::unordered_map<Key, Val, Hash, KeyEqual, Allocator>
      cached_conns_[kNumCores];
  std::unordered_map<Key, Val, Hash, KeyEqual, Allocator> global_conns_;
  rt::Spin global_spin_;
  std::function<tcpconn_t *(Key)> creator_;
  uint32_t per_core_cache_size_;
};
} // namespace nu

#include "impl/conn_mgr.ipp"

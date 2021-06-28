#pragma once

#include <functional>
#include <stack>
#include <unordered_map>
#include <utility>

extern "C" {
#include <runtime/tcp.h>
}
#include <net.h>
#include <sync.h>

#include "nu/defs.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/cached_pool.hpp"

namespace nu {

template <typename Key> class ConnectionManager {
public:
  ConnectionManager(const std::function<rt::TcpConn *(Key)> &creator,
                    uint32_t per_core_cache_size);
  ConnectionManager(std::function<rt::TcpConn *(Key)> &&creator,
                    uint32_t per_core_cache_size);
  ~ConnectionManager();
  rt::TcpConn *get_conn(Key k);
  void put_conn(Key k, rt::TcpConn *conn);
  void reserve_conns(Key k, uint32_t num);

private:
  using Val =
      std::stack<rt::TcpConn *,
                 std::vector<rt::TcpConn *, RuntimeAllocator<rt::TcpConn *>>>;
  using Hash = std::hash<Key>;
  using KeyEqual = std::equal_to<Key>;
  using Allocator = RuntimeAllocator<std::pair<const Key, Val>>;
  std::unordered_map<Key, Val, Hash, KeyEqual, Allocator>
      cached_conns_[kNumCores];
  std::unordered_map<Key, Val, Hash, KeyEqual, Allocator> global_conns_;
  rt::Spin global_spin_;
  std::function<rt::TcpConn *(Key)> creator_;
  uint32_t per_core_cache_size_;
};
} // namespace nu

#include "nu/impl/conn_mgr.ipp"

namespace nu {

inline tcpconn_t *RemObjConnManager::get_conn(RemObjID id) {
  auto addr = get_addr(id);
  auto conn = mgr_.get_conn(addr);
  BUG_ON(addr != tcp_remote_addr(conn));
  return conn;
}

inline void RemObjConnManager::put_conn(tcpconn_t *conn) {
  mgr_.put_conn(tcp_remote_addr(conn), conn);
}

inline void RemObjConnManager::reserve_conns(uint32_t num,
                                             netaddr obj_server_addr) {
  mgr_.reserve_conns(obj_server_addr, num);
}
} // namespace nu

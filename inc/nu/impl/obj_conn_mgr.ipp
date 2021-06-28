namespace nu {

inline rt::TcpConn *RemObjConnManager::get_conn(RemObjID id) {
  auto addr = get_addr(id);
  auto *conn = mgr_.get_conn(addr);
  BUG_ON(addr != conn->RemoteAddr());
  return conn;
}

inline void RemObjConnManager::put_conn(rt::TcpConn *conn) {
  mgr_.put_conn(conn->RemoteAddr(), conn);
}

inline void RemObjConnManager::reserve_conns(uint32_t num,
                                             netaddr obj_server_addr) {
  mgr_.reserve_conns(obj_server_addr, num);
}
} // namespace nu

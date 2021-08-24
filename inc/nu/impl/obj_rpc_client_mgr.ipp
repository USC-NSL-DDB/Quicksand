namespace nu {

inline RPCClient *RemObjRPCClientMgr::get(RemObjID id) {
  auto addr = get_addr(id);
  auto *client = mgr_.get_conn(addr);
  BUG_ON(addr != client->RemoteAddr());
  return client;
}

} // namespace nu

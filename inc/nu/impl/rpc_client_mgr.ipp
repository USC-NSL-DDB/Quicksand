namespace nu {

template <typename Key>
RPCClientMgr<Key>::RPCClientMgr(
    const std::function<std::unique_ptr<RPCClient> *(Key)> &creator)
    : creator_(creator) {}

template <typename Key>
RPCClientMgr<Key>::RPCClientMgr(std::function<rt::TcpConn *(Key)> &&creator)
    : creator_(std::move(creator)) {}

template <typename Key> RPCClient *RPCClientMgr<Key>::get(Key k) {
retry:
  auto *wrapper = map_.get(k);
  if (likely(wrapper)) {
    return wrapper->rpc_client.get();
  }

  map_.try_emplace(k, k, creator_);
  goto retry;
}

} // namespace nu

inline bool operator==(netaddr x, netaddr y) {
  return x.ip == y.ip && x.port == y.port;
}

namespace std {
inline std::size_t hash<netaddr>::operator()(const netaddr &k) const {
  return (static_cast<std::size_t>(k.ip) << 16) | k.port;
};
} // namespace std

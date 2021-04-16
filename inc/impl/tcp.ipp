namespace nu {

constexpr static uint32_t kStackBufSize = 128;
  
bool __tcp_read_until(tcpconn_t *c, void *buf, size_t expect);
bool __tcp_read2_until(tcpconn_t *c, void *buf_0, size_t expect_0, void *buf_1,
                       size_t expect_1);
bool __tcp_write_until(tcpconn_t *c, const void *buf, size_t expect);
bool __tcp_write2_until(tcpconn_t *c, const void *buf_0, size_t expect_0,
                        const void *buf_1, size_t expect_1);

inline bool tcp_read_until(tcpconn_t *c, void *buf, size_t expect) {
  if (unlikely(!expect)) {
    return true;
  }
  return __tcp_read_until(c, buf, expect);
}

inline bool tcp_read2_until(tcpconn_t *c, void *buf_0, size_t expect_0,
                            void *buf_1, size_t expect_1) {
  auto sum_expect = expect_0 + expect_1;
  if (likely(sum_expect <= kStackBufSize)) {
    uint8_t buf[kStackBufSize];
    auto ret = tcp_read_until(c, buf, sum_expect);
    __builtin_memcpy(buf_0, buf, expect_0);
    __builtin_memcpy(buf_1, &buf[expect_0], expect_1);
    return ret;
  }
  return __tcp_read2_until(c, buf_0, expect_0, buf_1, expect_1);
}

inline bool tcp_write_until(tcpconn_t *c, const void *buf, size_t expect) {
  if (unlikely(!expect)) {
    return true;
  }
  return __tcp_write_until(c, buf, expect);
}

inline bool tcp_write2_until(tcpconn_t *c, const void *buf_0, size_t expect_0,
                             const void *buf_1, size_t expect_1) {
  auto sum_expect = expect_0 + expect_1;
  if (likely(sum_expect <= kStackBufSize)) {
    uint8_t buf[kStackBufSize];
    __builtin_memcpy(buf, buf_0, expect_0);
    __builtin_memcpy(&buf[expect_0], buf_1, expect_1);
    return tcp_write_until(c, buf, sum_expect);
  }  
  return __tcp_write2_until(c, buf_0, expect_0, buf_1, expect_1);
}

} // namespace nu

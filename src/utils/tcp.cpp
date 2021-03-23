#include "utils/tcp.hpp"

namespace nu {

bool tcp_read_until(tcpconn_t *c, void *buf, size_t expect) {
  if (unlikely(!expect)) {
    return true;
  }

  ssize_t s_real = tcp_read(c, reinterpret_cast<uint8_t *>(buf), expect);
  if (unlikely(s_real <= 0)) {
    return false;
  }

  size_t real = s_real;
  if (unlikely(real != expect)) {
    do {
      ssize_t delta =
          tcp_read(c, reinterpret_cast<uint8_t *>(buf) + real, expect - real);
      if (unlikely(delta <= 0)) {
        return false;
      }
      real += delta;
    } while (real < expect);
  }
  return true;
}

bool tcp_write_until(tcpconn_t *c, const void *buf, size_t expect) {
  if (unlikely(!expect)) {
    return true;
  }

  ssize_t s_real = tcp_write(c, reinterpret_cast<const uint8_t *>(buf), expect);
  if (unlikely(s_real < 0)) {
    return false;
  }

  size_t real = s_real;
  if (unlikely(real != expect)) {
    do {
      ssize_t delta = tcp_write(
          c, reinterpret_cast<const uint8_t *>(buf) + real, expect - real);
      if (unlikely(delta < 0)) {
        return false;
      }
      real += delta;
    } while (real < expect);
  }
  return true;
}

bool tcp_write2_until(tcpconn_t *c, const void *buf_0, size_t expect_0,
                      const void *buf_1, size_t expect_1) {
  if (unlikely(expect_0 + expect_1 == 0)) {
    return true;
  } else if (unlikely(expect_0 == 0)) {
    tcp_write_until(c, buf_1, expect_1);
    return true;
  } else if (unlikely(expect_1 == 0)) {
    tcp_write_until(c, buf_0, expect_0);
    return true;
  }

  iovec iovecs[2];
  iovecs[0] = {.iov_base = const_cast<void *>(buf_0), .iov_len = expect_0};
  iovecs[1] = {.iov_base = const_cast<void *>(buf_1), .iov_len = expect_1};
  ssize_t s_real = tcp_writev(c, iovecs, 2);
  if (unlikely(s_real < 0)) {
    return false;
  }

  size_t real = s_real;
  if (unlikely(real != expect_0 + expect_1)) {
    do {
      ssize_t delta;
      if (likely(real >= expect_0)) {
        delta = tcp_write(
            c, reinterpret_cast<const uint8_t *>(buf_1) + (real - expect_0),
            expect_1 - (real - expect_0));
      } else {
        iovecs[0].iov_base = reinterpret_cast<void *>(
            reinterpret_cast<uint8_t *>(iovecs[0].iov_base) + real);
        iovecs[0].iov_len -= real;
        delta = tcp_writev(c, iovecs, 2);
      }
      if (unlikely(delta < 0)) {
        return false;
      }
      real += delta;
    } while (real < expect_0 + expect_1);
  }
  return true;
}
} // namespace nu

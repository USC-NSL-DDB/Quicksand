#pragma once

extern "C" {
#include <base/compiler.h>
#include <runtime/tcp.h>
}

#include <cstddef>

namespace nu {

bool tcp_read_until(tcpconn_t *c, void *buf, size_t expect);
bool tcp_write_until(tcpconn_t *c, const void *buf, size_t expect);
bool tcp_write2_until(tcpconn_t *c, const void *buf_0, size_t expect_0,
                      const void *buf_1, size_t expect_1);

} // namespace nu

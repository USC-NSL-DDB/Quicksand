/*
 * tcp.h - TCP sockets
 */

#pragma once

#include <runtime/net.h>
#include <sys/uio.h>
#include <sys/socket.h>

struct tcpqueue;
typedef struct tcpqueue tcpqueue_t;
struct tcpconn;
typedef struct tcpconn tcpconn_t;

extern int tcp_dial(struct netaddr laddr, struct netaddr raddr,
		    tcpconn_t **c_out);
extern int tcp_dial_affinity(uint32_t affinity, struct netaddr raddr,
		    tcpconn_t **c_out);
extern int tcp_dial_conn_affinity(tcpconn_t *in, struct netaddr raddr,
		    tcpconn_t **c_out);
extern int tcp_listen(struct netaddr laddr, int backlog, tcpqueue_t **q_out);
extern int tcp_accept(tcpqueue_t *q, tcpconn_t **c_out);
extern void tcp_qshutdown(tcpqueue_t *q);
extern void tcp_qclose(tcpqueue_t *q);
extern struct netaddr tcp_local_addr(tcpconn_t *c);
extern struct netaddr tcp_remote_addr(tcpconn_t *c);
extern int tcp_shutdown(tcpconn_t *c, int how);
extern void tcp_abort(tcpconn_t *c);
extern void tcp_close(tcpconn_t *c);

extern ssize_t __tcp_write(tcpconn_t *c, const void *buf, size_t len, bool nt);
extern ssize_t __tcp_writev(tcpconn_t *c, const struct iovec *iov, int iovcnt,
                            bool nt);
extern ssize_t __tcp_read(tcpconn_t *c, void *buf, size_t len, bool nt);
extern ssize_t __tcp_readv(tcpconn_t *c, const struct iovec *iov, int iovcnt,
                           bool nt);

/**
 * tcp_write - writes data to a TCP connection
 * @c: the TCP connection
 * @buf: a buffer from which to copy the data
 * @len: the length of the data
 *
 * Returns the number of bytes written (could be less than @len), or < 0
 * if there was a failure.
 */
static inline ssize_t tcp_write(tcpconn_t *c, const void *buf, size_t len)
{
	return __tcp_write(c, buf, len, false);
}

/**
 * tcp_write_nt - similar with tcp_write, but uses non-temporal memcpy
 */
static inline ssize_t tcp_write_nt(tcpconn_t *c, const void *buf, size_t len)
{
	return __tcp_write(c, buf, len, true);
}

/**
 * tcp_writev - writes vectored data to a TCP connection
 * @c: the TCP connection
 * @iov: a pointer to the IO vector
 * @iovcnt: the number of vectors in @iov
 *
 * Returns the number of bytes written (could be less than requested), or < 0
 * if there was a failure.
 */
static inline ssize_t tcp_writev(tcpconn_t *c, const struct iovec *iov,
				 int iovcnt)
{
	return __tcp_writev(c, iov, iovcnt, false);
}

/**
 * tcp_writev_nt - similar with tcp_writev, but uses non-temporal memcpy
 */
static inline ssize_t tcp_writev_nt(tcpconn_t *c, const struct iovec *iov,
				    int iovcnt)
{
	return __tcp_writev(c, iov, iovcnt, true);
}

/**
 * tcp_read - reads data from a TCP connection
 * @c: the TCP connection
 * @buf: a buffer to store the read data
 * @len: the length of @buf
 *
 * Returns the number of bytes read, 0 if the connection is closed, or < 0
 * if an error occurred.
 */
static inline ssize_t tcp_read(tcpconn_t *c, void *buf, size_t len)
{
	return __tcp_read(c, buf, len, false);
}

/**
 * tcp_read_nt - similar with tcp_read, but uses non-temporal memcpy
 */
static inline ssize_t tcp_read_nt(tcpconn_t *c, void *buf, size_t len)
{
	return __tcp_read(c, buf, len, true);
}

/**
 * tcp_readv - reads vectored data from a TCP connection
 * @c: the TCP connection
 * @iov: a pointer to the IO vector
 * @iovcnt: the number of vectors in @iov
 *
 * Returns the number of bytes read, 0 if the connection is closed, or < 0
 * if an error occurred.
 */
static inline ssize_t tcp_readv(tcpconn_t *c, const struct iovec *iov,
				int iovcnt)
{
	return __tcp_readv(c, iov, iovcnt, false);
}

/**
 * tcp_readv_nt - similar with tcp_readv, but uses non-temporal memcpy
 */
static inline ssize_t tcp_readv_nt(tcpconn_t *c, const struct iovec *iov,
				   int iovcnt)
{
	return __tcp_readv(c, iov, iovcnt, true);
}

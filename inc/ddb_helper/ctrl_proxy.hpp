#pragma once

#include <arpa/inet.h>
#include <endian.h>  // For be64toh
#include <netinet/in.h>
#include <pthread.h>
#include <sys/eventfd.h>  // For eventfd
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <syncstream>

struct ProcletCtrlHdr {
  uint32_t cmd;
  uint32_t len;
  uint64_t token;

  const static ProcletCtrlHdr from_req_hdr(const ProcletCtrlHdr& hdr,
                                           uint32_t reply_len) {
    return ProcletCtrlHdr{hdr.cmd, reply_len, hdr.token};
  }
};

struct ProcletQueryResp {
  uint64_t proclet_id;
  uint32_t proclet_ip;

  const static size_t SIZE = sizeof(proclet_id) + sizeof(proclet_ip);

  void serialize(std::span<std::byte> span) const {
    if (span.size() < SIZE) {
      throw std::runtime_error(
          "Serialization buffer too small for ProcletQueryResp");
    }
    // Convert to network byte order (big-endian) before copying
    uint64_t proclet_id_be = htobe64(proclet_id);
    uint32_t proclet_ip_be = htonl(proclet_ip);

    std::memcpy(span.data(), &proclet_id_be, sizeof(proclet_id_be));
    std::memcpy(span.data() + sizeof(proclet_id_be), &proclet_ip_be,
                sizeof(proclet_ip_be));
  }
};

const size_t HEADER_SIZE =
    sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t);  // 16 bytes

// --- Request Context ---
struct RequestContext {
  int client_fd;
  int completion_efd;
  // std::vector<uint8_t> request_data;  // Data received from the client
  ProcletCtrlHdr req_hdr;
  std::unique_ptr<std::byte[]> req_payload;

  // nu::ProcletID req_proclet_id;     // ProcletID extracted from the request
  // std::vector<uint8_t> reply_data;  // Data received from the target server
  ProcletCtrlHdr reply_hdr;
  std::unique_ptr<std::byte[]> reply_payload;

  std::atomic<bool> processing_complete;
  std::atomic<bool> error_occurred;

  RequestContext(int fd)
      : client_fd(fd),
        completion_efd(-1),
        processing_complete(false),
        error_occurred(false) {
    // Create the eventfd for this specific request
    // EFD_CLOEXEC: Close eventfd automatically on execve
    // EFD_SEMAPHORE (optional): Provides semaphore-like semantics for read
    // (value decreases by 1) We use the default behavior where read consumes
    // the entire value if >= 1.
    completion_efd = eventfd(0, EFD_CLOEXEC);
    if (completion_efd == -1) {
      perror("eventfd creation failed");
      error_occurred = true;
    }
    std::osyncstream ss_ctor(std::cout);
    ss_ctor << "[RequestContext Ctor]: Created context for fd " << client_fd
            << " (efd=" << completion_efd << ")" << std::endl;
    ss_ctor.emit();
  }

  ~RequestContext() {
    if (completion_efd != -1) {
      close(completion_efd);
    }
    std::osyncstream ss_dtor(std::cout);
    ss_dtor << "[RequestContext Dtor]: Cleaned up context for fd " << client_fd
            << " (efd=" << completion_efd << ")" << std::endl;
    ss_dtor.emit();
  }

  RequestContext(const RequestContext&) = delete;
  RequestContext& operator=(const RequestContext&) = delete;
};

/**
 * Receives exactly 'size' bytes into the buffer from the socket.
 * Handles partial reads.
 * Returns true on success, false on error or connection closed prematurely.
 */
inline bool receive_exact(int socket_fd, std::byte* buffer, size_t size) {
  size_t total_received = 0;
  while (total_received < size) {
    ssize_t bytes_received =
        read(socket_fd, buffer + total_received, size - total_received);

    if (bytes_received < 0) {
      // Error occurred
      perror("read failed");  // POSIX error reporting
      return false;
    } else if (bytes_received == 0) {
      // Connection closed gracefully by peer before all bytes were received
      std::cerr << "Connection closed by peer while receiving." << std::endl;
      return false;
    }
    total_received += static_cast<size_t>(bytes_received);
  }
  return true;  // Successfully received 'size' bytes
}

/**
 * Receives and deserializes the ProcletCtrlHdr from the given socket.
 * Throws std::runtime_error on failure.
 */
inline ProcletCtrlHdr read_header(int socket_fd) {
  auto header_buf = std::make_unique_for_overwrite<std::byte[]>(HEADER_SIZE);

  if (!receive_exact(socket_fd, header_buf.get(), HEADER_SIZE)) {
    throw std::runtime_error("Failed to receive complete header");
  }

  ProcletCtrlHdr header;
  size_t current_offset = 0;

  // cmd (uint32_t)
  uint32_t cmd_net;
  std::memcpy(&cmd_net, header_buf.get() + current_offset, sizeof(uint32_t));
  header.cmd = ntohl(cmd_net);
  current_offset += sizeof(uint32_t);

  // len (uint32_t)
  uint32_t len_net;
  std::memcpy(&len_net, header_buf.get() + current_offset, sizeof(uint32_t));
  header.len = ntohl(len_net);
  current_offset += sizeof(uint32_t);

  // token (uint64_t)
  uint64_t token_net;
  std::memcpy(&token_net, header_buf.get() + current_offset, sizeof(uint64_t));
  header.token = be64toh(token_net);

  return header;
}

/**
 * Sends exactly 'size' bytes from the buffer to the socket.
 * Handles partial writes.
 * Returns true on success, false on error.
 */
inline bool send_exact(int socket_fd, const unsigned char* buffer,
                       size_t size) {
  size_t total_sent = 0;
  while (total_sent < size) {
    ssize_t bytes_sent =
        write(socket_fd, buffer + total_sent, size - total_sent);

    if (bytes_sent < 0) {
      perror("write failed");
      return false;
    }

    if (bytes_sent == 0 && total_sent < size) {
      std::cerr << "Write returned 0 unexpectedly." << std::endl;
      return false;  // TODO: Or handle as appropriate (e.g., retry, check
                     // socket state)
    }
    total_sent += static_cast<size_t>(bytes_sent);
  }
  return true;
}

/**
 * Serializes and sends the ProcletCtrlHdr to the given socket.
 * Returns true on success, false on failure.
 */
inline bool write_header(int socket_fd, const ProcletCtrlHdr& header) {
  unsigned char header_buffer[HEADER_SIZE];
  size_t current_offset = 0;

  // cmd (uint32_t)
  uint32_t cmd_net = htonl(header.cmd);
  std::memcpy(header_buffer + current_offset, &cmd_net, sizeof(uint32_t));
  current_offset += sizeof(uint32_t);

  // len (uint32_t)
  uint32_t len_net = htonl(header.len);
  std::memcpy(header_buffer + current_offset, &len_net, sizeof(uint32_t));
  current_offset += sizeof(uint32_t);

  // token (uint64_t)
  uint64_t token_net = htobe64(header.token);
  std::memcpy(header_buffer + current_offset, &token_net, sizeof(uint64_t));

  if (!send_exact(socket_fd, header_buffer, HEADER_SIZE)) {
    std::cerr << "Failed to send complete header" << std::endl;
    return false;
  }
  return true;
}

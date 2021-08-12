#include <cstddef>
#include <functional>
#include <memory>
#include <queue>
#include <span>
#include <vector>

#include "net.h"
#include "sync.h"
#include "thread.h"

namespace nu {

// RPCReturnBuffer manages a return data buffer and its lifetime.
class RPCReturnBuffer {
 public:
  RPCReturnBuffer() {}
  RPCReturnBuffer(std::span<const std::byte> buf,
                  folly::Function<void()> deleter_fn)
      : buf_(buf), deleter_fn_(std::move(deleter_fn)) {}
  ~RPCReturnBuffer() {
    if (deleter_fn_) deleter_fn_();
  }

  // disable copy
  RPCReturnBuffer(const RPCReturnBuffer &) = delete;
  RPCReturnBuffer &operator=(const RPCReturnBuffer &) = delete;

  // support move
  RPCReturnBuffer(RPCReturnBuffer &&rbuf)
      : buf_(rbuf.buf_), deleter_fn_(std::move(rbuf.deleter_fn_)) {
    rbuf.buf_ = std::span<const std::byte>();
  }
  RPCReturnBuffer &operator=(RPCReturnBuffer &&rbuf) {
    buf_ = rbuf.buf_;
    deleter_fn_ = std::move(rbuf.deleter_fn_);
    buf_ = std::span<const std::byte>();
    return *this;
  }

  explicit operator bool() const { return !buf_.empty(); }

  // replaces the return data buffer.
  void Reset(std::span<const std::byte> buf,
             folly::Function<void()> deleter_fn) {
    if (deleter_fn_) deleter_fn_();
    buf_ = buf;
    deleter_fn_ = std::move(deleter_fn);
  }

  // Gets the return data buffer.
  std::span<const std::byte> get_buf() const { return buf_; }

 private:
  std::span<const std::byte> buf_;
  folly::Function<void()> deleter_fn_;
};

namespace rpc_internal {

// RPCCompletion manages the completion of an inflight request.
class RPCCompletion {
 public:
  RPCCompletion(RPCReturnBuffer *buf) : buf_(buf) { w_.Arm(); }
  ~RPCCompletion(){};

  // Complete the request with return data and wake the blocking thread.
  void Done(std::span<const std::byte> buf,
            folly::Function<void()> deleter_fn) {
    buf_->Reset(buf, std::move(deleter_fn));
    w_.Wake();
  }

  // Complete the request without return data and wake the blocking thread.
  void Done() { w_.Wake(); }

 private:
  RPCReturnBuffer *buf_;
  rt::ThreadWaker w_;
};

// RPCFlow encapsulates one of the TCP connections used by an RPCClient.
class RPCFlow {
 public:
  RPCFlow(std::unique_ptr<rt::TcpConn> c)
      : close_(false),
        c_(std::move(c)),
        sent_count_(0),
        recv_count_(0),
        credits_(128) {}
  ~RPCFlow();

  // A factory to create new flows with CPU affinity.
  static std::unique_ptr<RPCFlow> New(unsigned int cpu_affinity, netaddr raddr);

  // Make an RPC call over this flow.
  void Call(std::span<const std::byte> src, RPCCompletion *c);

  // Disable move and copy.
  RPCFlow(const RPCFlow &) = delete;
  RPCFlow &operator=(const RPCFlow &) = delete;

 private:
  // State for managing inflight requests.
  struct req_ctx {
    std::span<const std::byte> payload;
    RPCCompletion *completion;
  };

  // Internal worker threads for sending and receiving.
  void SendWorker();
  void ReceiveWorker();

  rt::Thread sender_, receiver_;
  rt::Spin lock_;
  bool close_;
  rt::ThreadWaker wake_sender_;
  std::unique_ptr<rt::TcpConn> c_;
  unsigned int sent_count_;
  unsigned int recv_count_;
  unsigned int credits_;
  std::queue<req_ctx> reqs_;
};

}  // namespace rpc_internal

// A function handler for each RPC request, invoked concurrently.
using RPCFuncPtr = RPCReturnBuffer (*)(std::span<const std::byte> args);
// Initializes and runs the RPC server.
void RPCServerInit(RPCFuncPtr fnptr);

class RPCClient {
 public:
  ~RPCClient(){};

  // Creates an RPC Client and establishes the underlying TCP connections.
  static std::unique_ptr<RPCClient> Dial(netaddr raddr);

  // Calls an RPC method.
  RPCReturnBuffer Call(std::span<const std::byte> args);

  // disable move and copy.
  RPCClient(const RPCClient &) = delete;
  RPCClient &operator=(const RPCClient &) = delete;

 private:
  using RPCCompletion = rpc_internal::RPCCompletion;
  using RPCFlow = rpc_internal::RPCFlow;

  RPCClient(std::vector<std::unique_ptr<RPCFlow>> flows)
      : flows_(std::move(flows)) {}

  // an array of per-kthread RPC flows.
  std::vector<std::unique_ptr<RPCFlow>> flows_;
};

}  // namespace nu

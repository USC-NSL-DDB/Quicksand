extern "C" {
#include <base/log.h>
}

#include <type_traits>

#include "rpc.h"
#include "runtime.h"

namespace nu {

namespace {

constexpr uint16_t kRPCPort = 9090;

// Command types for the RPC protocol.
enum rpc_cmd : unsigned int {
  call = 0,
  update,
};

// Binary header format for requests sent by client.
struct rpc_req_hdr {
  rpc_cmd cmd;                 // the command type
  unsigned int demand;         // number of RPCs waiting to be sent and inflight
  std::size_t len;             // the length of this RPC request
  std::size_t completion_data; // an opaque token to complete the RPC
};

constexpr rpc_req_hdr MakeCallRequest(unsigned int demand, std::size_t len,
                                      std::size_t completion_data) {
  return rpc_req_hdr{rpc_cmd::call, demand, len, completion_data};
}

constexpr rpc_req_hdr MakeUpdateRequest(unsigned int demand) {
  return rpc_req_hdr{rpc_cmd::update, demand, 0, 0};
}

// Binary header format for responses sent by server.
struct rpc_resp_hdr {
  rpc_cmd cmd;                 // the command type
  unsigned int credits;        // the number of credits available
  std::size_t len;             // the length of this RPC response
  std::size_t completion_data; // an opaque token to complete the RPC
};

constexpr rpc_resp_hdr MakeCallResponse(unsigned int credits, std::size_t len,
                                        std::size_t completion_data) {
  return rpc_resp_hdr{rpc_cmd::call, credits, len, completion_data};
}

constexpr rpc_resp_hdr MakeUpdateResponse(unsigned int credits) {
  return rpc_resp_hdr{rpc_cmd::update, credits, 0, 0};
}

class RPCServer {
 public:
  RPCServer(std::unique_ptr<rt::TcpConn> c, nu::RPCHandler &handler)
      : c_(std::move(c)), handler_(handler), close_(false) {}
  ~RPCServer() {}

  // Runs the RPCServer, returning when the connection is closed.
  void Run();
  // Sends the return results of an RPC.
  void Return(RPCReturnBuffer &&buf, std::size_t completion_data);

 private:
  // Internal worker threads for sending and receiving.
  void SendWorker();
  void ReceiveWorker();

  struct completion {
    RPCReturnBuffer buf;
    std::size_t completion_data;
  };

  rt::Spin lock_;
  std::unique_ptr<rt::TcpConn> c_;
  rt::ThreadWaker wake_sender_;
  std::vector<completion> completions_;
  float credits_;
  unsigned int demand_;
  nu::RPCHandler &handler_;
  bool close_;
};

void RPCServer::Run() {
  rt::Thread th([this] { SendWorker(); });
  ReceiveWorker();
  th.Join();
}

void RPCServer::Return(RPCReturnBuffer &&buf, std::size_t completion_data) {
  rt::SpinGuard guard(&lock_);
  completions_.emplace_back(std::move(buf), completion_data);
  wake_sender_.Wake();
}

void RPCServer::SendWorker() {
  std::vector<completion> completions;
  std::vector<iovec> iovecs;
  std::vector<rpc_resp_hdr> hdrs;

  while (true) {
    {
      // wait for an actionable state.
      rt::SpinGuard guard(&lock_);
      while (completions_.empty() && !close_) guard.Park(&wake_sender_);

      // gather all queued completions.
      std::move(completions_.begin(), completions_.end(),
                std::back_inserter(completions));
      completions_.clear();
    }

    // Check if the connection is closed.
    if (unlikely(close_ && completions.empty())) break;

    // process each of the requests.
    iovecs.clear();
    hdrs.clear();
    hdrs.reserve(completions.size());
    for (const auto &c : completions) {
      auto span = c.buf.get_buf();
      hdrs.emplace_back(
          MakeCallResponse(credits_, span.size_bytes(), c.completion_data));
      iovecs.emplace_back(&hdrs.back(), sizeof(decltype(hdrs)::value_type));
      if (span.size_bytes() == 0) continue;
      iovecs.emplace_back(const_cast<std::byte *>(span.data()),
                          span.size_bytes());
    }

    // send data on the wire.
    ssize_t ret = c_->WritevFull(std::span<const iovec>(iovecs));
    if (unlikely(ret <= 0)) {
      log_err("rpc: WritevFull failed, err = %ld", ret);
      return;
    }
    completions.clear();
  }
  if (WARN_ON(c_->Shutdown(SHUT_WR))) c_->Abort();
}

void RPCServer::ReceiveWorker() {
  while (true) {
    // Read the request header.
    rpc_req_hdr hdr;
    ssize_t ret = c_->ReadFull(&hdr, sizeof(hdr));
    if (unlikely(ret == 0)) break;
    if (unlikely(ret < 0)) {
      log_err("rpc: ReadFull failed, err = %ld", ret);
      break;
    }

    // Parse the request header.
    std::size_t completion_data = hdr.completion_data;
    demand_ = hdr.demand;
    if (hdr.cmd != rpc_cmd::call) continue;

    // Spawn a handler with no argument data provided.
    if (hdr.len == 0) {
      rt::Spawn([this, completion_data]() {
        Return(handler_(std::span<const std::byte>{}), completion_data);
      });
      continue;
    }

    // Allocate and fill a buffer with the argument data.
    auto buf = std::make_unique_for_overwrite<std::byte[]>(hdr.len);
    ret = c_->ReadFull(buf.get(), hdr.len);
    if (unlikely(ret == 0)) break;
    if (unlikely(ret < 0)) {
      log_err("rpc: ReadFull failed, err = %ld", ret);
      return;
    }

    // Spawn a handler with argument data provided.
    rt::Spawn([this, completion_data, b = std::move(buf),
               len = hdr.len]() mutable {
      Return(handler_(std::span<const std::byte>{b.get(), len}), completion_data);
    });
  }

  // Wake the sender to close the connection.
  {
    rt::SpinGuard guard(&lock_);
    close_ = true;
    wake_sender_.Wake();
  }
}

void RPCServerWorker(std::unique_ptr<rt::TcpConn> c, RPCHandler &handler) {
  RPCServer s(std::move(c), handler);
  s.Run();
}

void RPCServerListener(RPCHandler &handler) {
  std::unique_ptr<rt::TcpQueue> q(rt::TcpQueue::Listen({0, kRPCPort}, 4096));
  BUG_ON(!q);

  while (true) {
    std::unique_ptr<rt::TcpConn> c(q->Accept());
    rt::Thread([c = std::move(c), &handler]() mutable {
      RPCServerWorker(std::move(c), handler);
    }).Detach();
  }
}

}  // namespace

namespace rpc_internal {

RPCFlow::~RPCFlow() {
  {
    rt::SpinGuard guard(&lock_);
    close_ = true;
    wake_sender_.Wake();
  }
  sender_.Join();
  receiver_.Join();
}

void RPCFlow::Call(std::span<const std::byte> src, RPCCompletion *c) {
  assert_preempt_disabled();
  rt::SpinGuard guard(&lock_);
  reqs_.emplace(req_ctx{src, c});
  if (sent_count_ - recv_count_ < credits_) wake_sender_.Wake();
}

void RPCFlow::SendWorker() {
  std::vector<req_ctx> reqs;
  std::vector<iovec> iovecs;
  std::vector<rpc_req_hdr> hdrs;

  while (true) {
    unsigned int demand, inflight;
    bool close;

    {
      // wait for an actionable state.
      rt::SpinGuard guard(&lock_);
      inflight = sent_count_ - recv_count_;
      while ((reqs_.empty() || inflight >= credits_) &&
             !(close_ && reqs_.empty())) {
        guard.Park(&wake_sender_);
        inflight = sent_count_ - recv_count_;
      }

      // gather queued requests up to the credit limit.
      while (!reqs_.empty() && inflight < credits_) {
        reqs.emplace_back(reqs_.front());
        reqs_.pop();
        inflight++;
      }
      sent_count_ += reqs.size();
      close = close_ && reqs_.empty();
      demand = inflight;
    }

    // Check if it is time to close the connection.
    if (unlikely(close)) break;

    // construct a scatter-gather list for all the pending requests.
    iovecs.clear();
    hdrs.clear();
    hdrs.reserve(reqs.size());
    for (const auto &r : reqs) {
      auto &span = r.payload;
      hdrs.emplace_back(
          MakeCallRequest(demand, span.size_bytes(),
                          reinterpret_cast<std::size_t>(r.completion)));
      iovecs.emplace_back(&hdrs.back(), sizeof(decltype(hdrs)::value_type));
      if (span.size_bytes() == 0) continue;
      iovecs.emplace_back(const_cast<std::byte *>(span.data()),
                          span.size_bytes());
    }

    // send data on the wire.
    ssize_t ret = c_->WritevFull(std::span<const iovec>(iovecs));
    if (unlikely(ret <= 0)) {
      log_err("rpc: WritevFull failed, err = %ld", ret);
      return;
    }
    reqs.clear();
  }

  // send FIN on the wire.
  if (WARN_ON(c_->Shutdown(SHUT_WR))) c_->Abort();
}

void RPCFlow::ReceiveWorker() {
  while (true) {
    // Read the response header.
    rpc_resp_hdr hdr;
    ssize_t ret = c_->ReadFull(&hdr, sizeof(hdr));
    if (unlikely(ret <= 0)) {
      log_err("rpc: ReadFull failed, err = %ld", ret);
      return;
    }

    // Check if we should wake the sender.
    {
      rt::SpinGuard guard(&lock_);
      unsigned int inflight = sent_count_ - ++recv_count_;
      // credits_ = hdr.credits;
      if (credits_ > inflight && !reqs_.empty()) wake_sender_.Wake();
    }

    if (hdr.cmd != rpc_cmd::call) continue;

    // Check if there is no return data.
    auto *completion = reinterpret_cast<RPCCompletion *>(hdr.completion_data);
    if (hdr.len == 0) {
      completion->Done();
      continue;
    }

    // Allocate and fill a buffer for the return data.
    auto buf = std::make_unique_for_overwrite<std::byte[]>(hdr.len);
    ret = c_->ReadFull(buf.get(), hdr.len);
    if (unlikely(ret <= 0)) {
      log_err("rpc: ReadFull failed, err = %ld", ret);
      return;
    }

    // Issue a completion, waking the blocked thread.
    std::span<const std::byte> s(buf.get(), hdr.len);
    completion->Done(s, [b = std::move(buf)]() mutable {});
  }
}

std::unique_ptr<RPCFlow> RPCFlow::New(unsigned int cpu_affinity,
                                      netaddr raddr) {
  raddr.port = kRPCPort;
  std::unique_ptr<rt::TcpConn> c(
      rt::TcpConn::DialAffinity(cpu_affinity, raddr));
  BUG_ON(!c);
  std::unique_ptr<RPCFlow> f = std::make_unique<RPCFlow>(std::move(c));
  f->sender_ = rt::Thread([f = f.get()] { f->SendWorker(); });
  f->receiver_ = rt::Thread([f = f.get()] { f->ReceiveWorker(); });
  return f;
}

}  // namespace rpc_internal

void RPCServerInit(RPCHandler &handler) { RPCServerListener(handler); }

void RPCServerInit(RPCHandler &&handler) { RPCServerListener(handler); }

std::unique_ptr<RPCClient> RPCClient::Dial(netaddr raddr) {
  std::vector<std::unique_ptr<RPCFlow>> v;
  for (unsigned int i = 0; i < rt::RuntimeMaxCores(); ++i) {
    v.emplace_back(RPCFlow::New(i, raddr));
  }
  return std::unique_ptr<RPCClient>(new RPCClient(std::move(v)));
}

RPCReturnBuffer RPCClient::Call(std::span<const std::byte> src) {
  RPCReturnBuffer buf;
  RPCCompletion completion(&buf);
  {
    rt::Preempt p;
    rt::PreemptGuardAndPark guard(&p);
    flows_[p.get_cpu()]->Call(src, &completion);
  }
  return buf;
}

}  // namespace nu

#include <cstdint>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
#include <net/ip.h>
#include <runtime/tcp.h>
}
#include "thread.h"

#include "defs.hpp"
#include "heap_mgr.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"
#include "utils/tcp.hpp"

namespace nu {

ThreadSafeHashMap<RemObjID, int,
                  RuntimeAllocator<std::pair<const RemObjID, int>>>
    obj_ref_cnts;

ObjServer::ObjServer() {}

ObjServer::~ObjServer() {
  if (tcp_queue_) {
    tcp_qshutdown(tcp_queue_);
    tcp_qclose(tcp_queue_);
  }
}

ObjServer::ObjServer(uint16_t port) { init(port); }

void ObjServer::init(uint16_t port) {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = port};
  BUG_ON(tcp_listen(addr, kTCPListenBackLog, &tcp_queue_) != 0);
}

void ObjServer::run_loop() {
  tcpconn_t *c;
  while (tcp_accept(tcp_queue_, &c) == 0) {
    rt::Thread([&, c]() { handle_reqs(c); }).Detach();
  }
}

void ObjServer::handle_reqs(tcpconn_t *c) {
  while (true) {
    uint64_t len;
    if (!tcp_read_until(c, &len, sizeof(len))) {
      break;
    }
    std::string args_str(len, '\0');
    if (!tcp_read_until(c, args_str.data(), len)) {
      break;
    }
    std::stringstream args_ss(std::move(args_str));
    cereal::BinaryInputArchive ia(args_ss);
    GenericHandler handler;
    ia >> handler;

    std::stringstream ret_ss;
    cereal::BinaryOutputArchive oa(ret_ss);
    handler(ia, oa);

    auto ret_str = ret_ss.str();
    uint64_t ret_size = ret_str.size();
    if (!tcp_write2_until(c, &ret_size, sizeof(ret_size), ret_str.data(),
                          ret_size)) {
      break;
    }
  }
}

void ObjServer::update_ref_cnt(cereal::BinaryInputArchive &ia,
                               cereal::BinaryOutputArchive &oa) {
  RemObjID id;
  ia >> id;
  int delta;
  ia >> delta;
  auto &cnt = obj_ref_cnts.get(id);
  cnt += delta;
  if (cnt == 0) {
    auto *heap_base = to_heap_base(id);
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
    heap_header->~HeapHeader();
    Runtime::heap_manager->free(heap_base);
    obj_ref_cnts.remove(id);
  }
}

} // namespace nu

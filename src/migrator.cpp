#include <fcntl.h>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>

extern "C" {
#include <base/assert.h>
#include <net/ip.h>
}
#include "thread.h"
#define __user
#include "ksched.h"

#include "ctrl_client.hpp"
#include "defs.hpp"
#include "heap_mgr.hpp"
#include "migrator.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"
#include "utils/tcp.hpp"

namespace nu {

std::function<tcpconn_t *(netaddr)> MigratorConnManager::creator_ =
    [](netaddr server_addr) {
      tcpconn_t *c;
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      BUG_ON(tcp_dial(local_addr, server_addr, &c) != 0);
      return c;
    };

MigratorConnManager::MigratorConnManager()
    : ConnectionManager<netaddr>(creator_) {}

Migrator::~Migrator() { BUG(); }

void Migrator::run_loader_loop(uint16_t loader_port) {
  rt::Thread([&]() {
    netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = loader_port};
    BUG_ON(tcp_listen(addr, kTCPListenBackLog, &loader_tcp_queue_) != 0);
    tcpconn_t *c;
    while (tcp_accept(loader_tcp_queue_, &c) == 0) {
      rt::Thread([&, c] { load(c); }).Detach();
    }
  })
      .Detach();
}

void Migrator::transmit_heap(tcpconn_t *c, HeapHeader *heap_header) {
  int obj_ref_cnt = heap_header->ref_cnt;
  tcp_write2_until(c, &heap_header, sizeof(heap_header), &obj_ref_cnt,
                   sizeof(obj_ref_cnt));
  const auto &slab = heap_header->slab;
  tcp_write_until(c, &slab, sizeof(slab));
  tcp_write_until(c, slab.get_base(), slab.get_usage());
}

void Migrator::transmit_threads(tcpconn_t *c, const auto &threads) {
  uint64_t num_threads = threads.size();
  tcp_write_until(c, &num_threads, sizeof(num_threads));
  for (auto thread : threads) {
    size_t tf_size;
    auto *tf = thread_get_trap_frame(thread, &tf_size);
    tcp_write_until(c, tf, tf_size);
    void *stack_range[2];
    thread_get_obj_stack(thread, &stack_range[1], &stack_range[0]);
    tcp_write2_until(c, stack_range, sizeof(stack_range), stack_range[0],
                     reinterpret_cast<uintptr_t>(stack_range[1]) -
                         reinterpret_cast<uintptr_t>(stack_range[0]) + 1);
    thread_mark_migrated(thread);
  }
}

void Migrator::forward_to_client(uint32_t num_rpcs,
                                 tcpconn_t *conn_to_new_server) {
  for (size_t i = 0; i < num_rpcs; i++) {
    tcpconn_t *conn_to_client;
    ObjRPCRespHdr hdr;
    tcp_read2_until(conn_to_new_server, &conn_to_client, sizeof(conn_to_client),
                    &hdr, sizeof(hdr));
    auto payload = std::make_unique<uint8_t[]>(hdr.payload_size);
    tcp_read_until(conn_to_new_server, payload.get(), hdr.payload_size);
    tcp_write2_until(conn_to_client, &hdr, sizeof(hdr), payload.get(),
                     hdr.payload_size);
    BUG_ON(tcp_shutdown(conn_to_client, SHUT_RDWR) < 0);
    tcp_close(conn_to_client);
  }
}

void Migrator::transmit_and_forward(netaddr dest_addr, void *heap_base) {
  auto conn = conn_mgr_.get_conn(dest_addr);

  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  transmit_heap(conn, heap_header);

  auto tmp_threads = heap_header->threads->all_keys();
  std::vector<thread_t *> threads;
  for (auto thread : tmp_threads) {
    threads.push_back(thread);
  }
  transmit_threads(conn, threads);

  forward_to_client(threads.size(), conn);

  conn_mgr_.put_conn(dest_addr, conn);
}

void Migrator::migrate(std::list<void *> heaps) {
  Resource resource;
  __builtin_memset(&resource, 0, sizeof(resource));

  auto iter = heaps.begin();
  while (iter != heaps.end()) {
    auto heap = *iter;
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap);
    Runtime::heap_manager->remove(heap);
    Runtime::heap_manager->rcu_synchronize();
    if (unlikely(!heap_header->ref_cnt)) {
      iter = heaps.erase(iter);
      Runtime::heap_manager->deallocate(heap);
      continue;
    }
    resource.mem_mbs += heap_header->slab.get_usage() >> 20;
    auto all_threads = heap_header->threads->all_keys();
    for (auto thread : all_threads) {
      thread_mark_migrating(thread);
    }
    ++iter;
  }
  pause_migrating_threads();

  auto optional_dest_addr =
      Runtime::controller_client->get_migration_dest(resource);
  BUG_ON(!optional_dest_addr);
  std::vector<rt::Thread> threads;
  for (auto heap : heaps) {
    threads.emplace_back(
        [&] { transmit_and_forward(*optional_dest_addr, heap); });
  }
  for (auto &thread : threads) {
    thread.Join();
  }

  gc_migrated_threads();
  for (auto heap : heaps) {
    Runtime::heap_manager->deallocate(heap);
  }
}

void *Migrator::load_heap(tcpconn_t *c, rt::Mutex *loader_mutex) {
  HeapHeader *heap_header;
  int obj_ref_cnt;
  tcp_read2_until(c, &heap_header, sizeof(heap_header), &obj_ref_cnt,
                  sizeof(obj_ref_cnt));
  // Only allows one loading at a time.
  loader_mutex->Lock();
  Runtime::heap_manager->allocate(heap_header);
  auto &slab = heap_header->slab;
  tcp_read_until(c, &slab, sizeof(slab));
  tcp_read_until(c, slab.get_base(), slab.get_usage());
  heap_header->ref_cnt = obj_ref_cnt;
  return heap_header;
}

void Migrator::load_threads(tcpconn_t *c, void *heap_base) {
  auto *slab = &reinterpret_cast<HeapHeader *>(heap_base)->slab;

  uint64_t num_threads;
  tcp_read_until(c, &num_threads, sizeof(num_threads));
  ACCESS_ONCE(remaining_forwarding_cnts_) = num_threads;

  for (uint64_t i = 0; i < num_threads; i++) {
    size_t tf_size;
    thread_get_trap_frame(thread_self(), &tf_size);
    auto tf = std::make_unique<uint8_t[]>(tf_size);
    tcp_read_until(c, tf.get(), tf_size);
    void *stack_range[2];
    tcp_read_until(c, stack_range, sizeof(stack_range));
    tcp_read_until(c, stack_range[0],
                   reinterpret_cast<uintptr_t>(stack_range[1]) -
                       reinterpret_cast<uintptr_t>(stack_range[0]) + 1);
    resume_migrated_thread(tf.get(), reinterpret_cast<uint64_t>(slab));
  }
}

void Migrator::load(tcpconn_t *c) {
  while (true) {    
    auto heap_base = load_heap(c, &loader_mutex_);
    loader_conn_ = c;
    load_threads(c, heap_base);

    Runtime::heap_manager->insert(heap_base);
    Runtime::controller_client->update_location(
        to_obj_id(heap_base), Runtime::obj_server->get_addr());

    forwarding_mutex_.Lock();
    while (ACCESS_ONCE(remaining_forwarding_cnts_)) {
      loader_done_forwarding_.Wait(&forwarding_mutex_);
    }
    forwarding_mutex_.Unlock();
    loader_mutex_.Unlock();
  }
}

void Migrator::forward_to_original_server(const ObjRPCRespHdr &hdr,
                                          const void *payload,
                                          tcpconn_t *conn_to_client) {
  rt::ScopedLock<rt::Mutex> lock(&forwarding_mutex_);
  tcp_write2_until(loader_conn_, &conn_to_client, sizeof(conn_to_client), &hdr,
                   sizeof(hdr));
  tcp_write_until(loader_conn_, payload, hdr.payload_size);

  barrier();
  if (--remaining_forwarding_cnts_ == 0) {
    loader_done_forwarding_.Signal();
  }
}

} // namespace nu

#include <algorithm>
#include <fcntl.h>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>

extern "C" {
#include <base/assert.h>
#include <net/ip.h>
#include <runtime/timer.h>
}
#include <thread.h>

#include "cond_var.hpp"
#include "ctrl_client.hpp"
#include "defs.hpp"
#include "heap_mgr.hpp"
#include "migrator.hpp"
#include "mutex.hpp"
#include "obj_conn_mgr.hpp"
#include "obj_server.hpp"
#include "runtime.hpp"
#include "runtime_alloc.hpp"
#include "utils/tcp.hpp"

namespace nu {

std::function<rt::TcpConn *(netaddr)> MigratorConnManager::creator_ =
    [](netaddr server_addr) {
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      auto *c = rt::TcpConn::Dial(local_addr, server_addr);
      BUG_ON(!c);
      return c;
    };

MigratorConnManager::MigratorConnManager()
    : ConnectionManager<netaddr>(creator_, 0) {}

Migrator::~Migrator() { BUG(); }

inline void Migrator::handle_load(rt::TcpConn *c) { load(c); }

void Migrator::handle_reserve_conns(rt::TcpConn *c) {
  RPCReqReserveConns req;
  BUG_ON(c->ReadFull(&req, sizeof(req)) <= 0);
  reserve_conns(req.num, req.dest_server_addr);
}

void Migrator::handle_copy(rt::TcpConn *c) {
  RPCReqCopy req;
  BUG_ON(c->ReadFull(&req, sizeof(req)) <= 0);
  BUG_ON(c->ReadFull(reinterpret_cast<uint8_t *>(req.start_addr), req.len) <=
         0);
  req.wg->Done();
}

void Migrator::run_loop(uint16_t port) {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = port};
  auto tcp_queue = rt::TcpQueue::Listen(addr, kTCPListenBackLog);
  tcp_queue_.reset(tcp_queue);
  rt::TcpConn *c;

  while ((c = tcp_queue_->Accept())) {
    rt::Thread([&, c] {
      std::unique_ptr<rt::TcpConn> gc(c);

      while (true) {
        uint8_t type;
        if (unlikely(c->ReadFull(&type, sizeof(type)) <= 0)) {
          break;
        }

        switch (type) {
        case LOAD:
          handle_load(c);
          break;
        case RESERVE_CONNS:
          handle_reserve_conns(c);
          break;
        case COPY:
          handle_copy(c);
          break;
        default:
          BUG();
        }
      }
      BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
    }).Detach();
  }
}

void Migrator::parallel_transmit_heap(netaddr dest_addr, rt::WaitGroup *wg,
                                      uint64_t start_addr, uint64_t len) {
  std::vector<rt::Thread> threads;
  threads.reserve(kTransmitHeapNumThreads);

  for (uint32_t i = 0; i < kTransmitHeapNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      auto conn = conn_mgr_.get_conn(dest_addr);
      uint8_t type = COPY;
      RPCReqCopy req;
      auto per_thread_len = (len - 1) / kTransmitHeapNumThreads + 1;
      req.start_addr = start_addr + tid * per_thread_len;
      req.len = (tid != kTransmitHeapNumThreads - 1)
                    ? per_thread_len
                    : start_addr + len - req.start_addr;
      req.wg = wg;
      const iovec iovecs[] = {{&type, sizeof(type)}, {&req, sizeof(req)}};
      BUG_ON(conn->WritevFull(std::span(iovecs)) < 0);
      BUG_ON(conn->WriteFull(reinterpret_cast<uint8_t *>(req.start_addr),
                             req.len) < 0);
      conn_mgr_.put_conn(dest_addr, conn);
    });
  }
  for (auto &thread : threads) {
    thread.Join();
  }
}

void Migrator::transmit_heap(rt::TcpConn *c, netaddr dest_addr,
                             HeapHeader *heap_header) {
  uint8_t type = LOAD;
  BUG_ON(c->WriteFull(&type, sizeof(type)) < 0);
  int obj_ref_cnt = heap_header->ref_cnt;
  const iovec iovecs0[] = {{&heap_header, sizeof(heap_header)},
                           {&obj_ref_cnt, sizeof(obj_ref_cnt)}};
  BUG_ON(c->WritevFull(std::span(iovecs0)) < 0);
  const auto &slab = heap_header->slab;
  auto start_addr = reinterpret_cast<uint64_t>(&slab);
  auto len = (reinterpret_cast<intptr_t>(slab.get_base()) -
              reinterpret_cast<intptr_t>(&slab)) +
             slab.get_usage();
  uint8_t ack;
  rt::WaitGroup *wg;
  const iovec iovecs1[] = {{&ack, sizeof(ack)}, {&wg, sizeof(wg)}};
  BUG_ON(c->ReadvFull(std::span(iovecs1)) <= 0);
  parallel_transmit_heap(dest_addr, wg, start_addr, len);
}

void Migrator::transmit_mutexes(rt::TcpConn *c, HeapHeader *heap_header,
                                std::unordered_set<thread_t *> *mutex_threads) {
  auto mutexes = heap_header->mutexes->all_keys();
  size_t num_mutexes = mutexes.size();

  if (num_mutexes) {
    const iovec iovecs[] = {{&num_mutexes, sizeof(num_mutexes)},
                            {mutexes.data(), num_mutexes * sizeof(Mutex *)}};
    BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  } else {
    BUG_ON(c->WriteFull(&num_mutexes, sizeof(num_mutexes)) < 0);
  }

  for (auto mutex : mutexes) {
    std::vector<thread_t *> ths;
    auto *waiters = mutex->get_waiters();
    void *th_raw;
    list_for_each_off(waiters, th_raw, thread_link_offset) {
      auto *th = reinterpret_cast<thread_t *>(th_raw);
      ths.push_back(th);
      mutex_threads->emplace(th);
    }
    size_t num_threads = ths.size();
    BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads)) < 0);
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
  }
}

void Migrator::transmit_condvars(
    rt::TcpConn *c, HeapHeader *heap_header,
    std::unordered_set<thread_t *> *condvar_threads) {
  auto condvars = heap_header->condvars->all_keys();
  size_t num_condvars = condvars.size();

  if (num_condvars) {
    const iovec iovecs[] = {
        {&num_condvars, sizeof(num_condvars)},
        {condvars.data(), num_condvars * sizeof(CondVar *)}};
    BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  } else {
    BUG_ON(c->WriteFull(&num_condvars, sizeof(num_condvars)) < 0);
  }

  for (auto condvar : condvars) {
    std::vector<thread_t *> ths;
    auto *waiters = condvar->get_waiters();
    void *th_raw;
    list_for_each_off(waiters, th_raw, thread_link_offset) {
      auto *th = reinterpret_cast<thread_t *>(th_raw);
      ths.push_back(th);
      condvar_threads->emplace(th);
    }
    size_t num_threads = ths.size();
    BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads)) < 0);
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
  }
}

void Migrator::transmit_time(rt::TcpConn *c, HeapHeader *heap_header,
                             std::unordered_set<thread_t *> *time_threads) {
  uint64_t migrator_tsc = rdtscp(nullptr) - start_tsc;
  int64_t sum_tsc = migrator_tsc + heap_header->time->offset_tsc_;

  const auto &timer_entries_list = heap_header->time->entries_;
  size_t num_entries = timer_entries_list.size();
  const iovec iovecs[] = {{&sum_tsc, sizeof(sum_tsc)},
                          {&num_entries, sizeof(num_entries)}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);

  auto timer_entries_arr = std::make_unique<timer_entry *[]>(num_entries);
  std::copy(timer_entries_list.begin(), timer_entries_list.end(),
            timer_entries_arr.get());
  BUG_ON(c->WriteFull(timer_entries_arr.get(),
                      sizeof(timer_entry *) * num_entries) < 0);

  for (size_t i = 0; i < num_entries; i++) {
    auto *entry = timer_entries_arr[i];
    auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
    time_threads->emplace(arg->th);
    transmit_one_thread(c, arg->th);
  }
}

void Migrator::transmit_one_thread(rt::TcpConn *c, thread_t *thread) {
  size_t tf_size;
  auto *tf = thread_get_trap_frame(thread, &tf_size);
  BUG_ON(c->WriteFull(tf, tf_size) < 0);
  void *stack_range[2];
  thread_get_obj_stack(thread, &stack_range[1], &stack_range[0]);
  const iovec iovecs[] = {
      {stack_range, sizeof(stack_range)},
      {stack_range[0], reinterpret_cast<uintptr_t>(stack_range[1]) -
                           reinterpret_cast<uintptr_t>(stack_range[0]) + 1}};
  BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  thread_mark_migrated(thread);
}

void Migrator::transmit_threads(rt::TcpConn *c,
                                const std::vector<thread_t *> &threads) {
  uint64_t num_threads = threads.size();
  BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads)) < 0);
  for (auto thread : threads) {
    transmit_one_thread(c, thread);
  }
}

void Migrator::forward_to_client(uint32_t num_rpcs,
                                 rt::TcpConn *conn_to_new_server) {
  for (size_t i = 0; i < num_rpcs; i++) {
    rt::TcpConn *conn_to_client;
    ObjRPCRespHdr hdr;
    const iovec iovecs[] = {{&conn_to_client, sizeof(conn_to_client)},
                            {&hdr, sizeof(hdr)}};
    BUG_ON(conn_to_new_server->ReadvFull(std::span(iovecs)) <= 0);
    auto payload = std::make_unique<uint8_t[]>(hdr.payload_size);
    if (hdr.payload_size) {
      BUG_ON(conn_to_new_server->ReadFull(payload.get(), hdr.payload_size) <=
             0);
    }
    if (hdr.payload_size) {
      const iovec iovecs[] = {{&hdr, sizeof(hdr)},
                              {payload.get(), hdr.payload_size}};
      BUG_ON(conn_to_client->WritevFull(std::span(iovecs)) < 0);
    } else {
      BUG_ON(conn_to_client->WriteFull(&hdr, sizeof(hdr)) < 0);
    }
    rt::Thread([&, conn_to_client] {
      Runtime::obj_server->handle_reqs(conn_to_client);
    }).Detach();
  }
}

void Migrator::transmit_and_forward(netaddr dest_addr, void *heap_base) {
  auto conn = conn_mgr_.get_conn(dest_addr);

  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  transmit_heap(conn, dest_addr, heap_header);

  std::unordered_set<thread_t *> blocked_threads;
  transmit_mutexes(conn, heap_header, &blocked_threads);
  transmit_condvars(conn, heap_header, &blocked_threads);
  transmit_time(conn, heap_header, &blocked_threads);

  auto all_threads = heap_header->threads->all_keys();
  std::vector<thread_t *> running_threads;
  for (auto thread : all_threads) {
    if (!blocked_threads.contains(thread)) {
      running_threads.push_back(thread);
    }
  }
  transmit_threads(conn, running_threads);

  forward_to_client(all_threads.size(), conn);

  conn_mgr_.put_conn(dest_addr, conn);
}

void Migrator::migrate(std::list<void *> heaps) {
  Resource resource;
  __builtin_memset(&resource, 0, sizeof(resource));

  auto iter = heaps.begin();
  while (iter != heaps.end()) {
    auto heap = *iter;
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap);
    if (!Runtime::heap_manager->remove(heap)) {
      iter = heaps.erase(iter);
      continue;
    }
    Runtime::heap_manager->rcu_writer_sync();
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
        [&, heap] { transmit_and_forward(*optional_dest_addr, heap); });
  }
  for (auto &thread : threads) {
    thread.Join();
  }

  gc_migrated_threads();
  for (auto heap : heaps) {
    Runtime::heap_manager->deallocate(heap);
  }
}

void *Migrator::load_heap(rt::TcpConn *c, rt::Mutex *loader_mutex) {
  HeapHeader *heap_header;
  int obj_ref_cnt;
  const iovec iovecs0[] = {{&heap_header, sizeof(heap_header)},
                           {&obj_ref_cnt, sizeof(obj_ref_cnt)}};
  if (c->ReadvFull(std::span(iovecs0)) <= 0) {
    return nullptr;
  }

  // Only allows one loading at a time.
  loader_mutex->Lock();
  Runtime::heap_manager->mmap(heap_header);
  uint8_t ack = true;
  rt::WaitGroup wg(kTransmitHeapNumThreads);
  auto *wg_p = &wg;
  const iovec iovecs1[] = {{&ack, sizeof(ack)}, {&wg_p, sizeof(wg_p)}};
  BUG_ON(c->WritevFull(std::span(iovecs1)) < 0);
  Runtime::heap_manager->setup(heap_header, /* migratable = */ true,
                               /* skip_slab = */ true);
  heap_header->ref_cnt = obj_ref_cnt;
  wg.Wait();

  return heap_header;
}

thread_t *Migrator::load_one_thread(rt::TcpConn *c, HeapHeader *heap_header) {
  auto tlsvar = reinterpret_cast<uint64_t>(&heap_header->slab);

  size_t tf_size;
  thread_get_trap_frame(thread_self(), &tf_size);
  auto tf = std::make_unique<uint8_t[]>(tf_size);
  BUG_ON(c->ReadFull(tf.get(), tf_size) <= 0);
  void *stack_range[2];
  BUG_ON(c->ReadFull(stack_range, sizeof(stack_range)) <= 0);
  BUG_ON(c->ReadFull(stack_range[0],
                     reinterpret_cast<uintptr_t>(stack_range[1]) -
                         reinterpret_cast<uintptr_t>(stack_range[0]) + 1) <= 0);
  return create_migrated_thread(tf.get(), tlsvar);
}

void Migrator::load_mutexes(rt::TcpConn *c, HeapHeader *heap_header) {
  size_t num_mutexes;
  BUG_ON(c->ReadFull(&num_mutexes, sizeof(num_mutexes)) <= 0);

  if (num_mutexes) {
    auto mutexes = std::make_unique<Mutex *[]>(num_mutexes);
    BUG_ON(c->ReadFull(mutexes.get(), num_mutexes * sizeof(Mutex *)) <= 0);

    for (size_t i = 0; i < num_mutexes; i++) {
      auto mutex = mutexes[i];
      heap_header->mutexes->put(mutex);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);

      auto *waiters = mutex->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto *th = load_one_thread(c, heap_header);
        auto *th_link = reinterpret_cast<list_node *>(
            reinterpret_cast<uintptr_t>(th) + thread_link_offset);
        list_add_tail(waiters, th_link);
      }
    }
  }
}

void Migrator::load_condvars(rt::TcpConn *c, HeapHeader *heap_header) {
  size_t num_condvars;
  BUG_ON(c->ReadFull(&num_condvars, sizeof(num_condvars)) <= 0);

  if (num_condvars) {
    auto condvars = std::make_unique<CondVar *[]>(num_condvars);
    BUG_ON(c->ReadFull(condvars.get(), num_condvars * sizeof(CondVar *)) <= 0);

    for (size_t i = 0; i < num_condvars; i++) {
      auto condvar = condvars[i];
      heap_header->condvars->put(condvar);

      auto *waiters = condvar->get_waiters();
      list_head_init(waiters);
      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);
      for (size_t j = 0; j < num_threads; j++) {
        auto *th = load_one_thread(c, heap_header);
        auto *th_link = reinterpret_cast<list_node *>(
            reinterpret_cast<uintptr_t>(th) + thread_link_offset);
        list_add_tail(waiters, th_link);
      }
    }
  }
}

void Migrator::load_time(rt::TcpConn *c, HeapHeader *heap_header) {
  auto *time = heap_header->time.get();

  int64_t sum_tsc;
  size_t num_entries;
  const iovec iovecs[] = {{&sum_tsc, sizeof(sum_tsc)},
                          {&num_entries, sizeof(num_entries)}};
  BUG_ON(c->ReadvFull(std::span(iovecs)) <= 0);
  auto loader_tsc = rdtscp(nullptr) - start_tsc;
  time->offset_tsc_ = sum_tsc - loader_tsc;

  if (num_entries) {
    auto timer_entries = std::make_unique<timer_entry *[]>(num_entries);
    BUG_ON(c->ReadFull(timer_entries.get(),
                       sizeof(timer_entry *) * num_entries) <= 0);

    for (size_t i = 0; i < num_entries; i++) {
      auto *entry = timer_entries[i];
      auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
      auto *th = load_one_thread(c, heap_header);
      arg->th = th;
      time->spin_.Lock();
      time->entries_.push_back(entry);
      arg->iter = --time->entries_.end();
      time->spin_.Unlock();
      entry->armed = false;
      timer_start(entry, time->to_physical_us(arg->logical_deadline_us));
    }
  }
}

void Migrator::load_threads(rt::TcpConn *c, HeapHeader *heap_header) {
  uint64_t num_threads;
  BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);

  ACCESS_ONCE(remaining_forwarding_cnts_) = num_threads;

  for (uint64_t i = 0; i < num_threads; i++) {
    auto *th = load_one_thread(c, heap_header);
    heap_header->threads->put(th);
    thread_ready(th);
  }
}

void Migrator::load(rt::TcpConn *c) {
  auto *heap_base = load_heap(c, &loader_mutex_);
  if (!heap_base) {
    return;
  }

  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base);
  ACCESS_ONCE(heap_header->migratable) = false;
  loader_conn_ = c;

  load_mutexes(c, heap_header);
  load_condvars(c, heap_header);
  load_time(c, heap_header);

  Runtime::heap_manager->insert(heap_base);

  rt::Thread([heap_base] {
    Runtime::controller_client->update_location(
        to_obj_id(heap_base), Runtime::obj_server->get_addr());
  }).Detach();

  load_threads(c, heap_header);

  forwarding_mutex_.Lock();
  while (ACCESS_ONCE(remaining_forwarding_cnts_)) {
    loader_done_forwarding_.Wait(&forwarding_mutex_);
  }

  ACCESS_ONCE(heap_header->migratable) = true;
  forwarding_mutex_.Unlock();
  loader_mutex_.Unlock();
}

void Migrator::forward_to_original_server(const ObjRPCRespHdr &hdr,
                                          const void *payload,
                                          rt::TcpConn *conn_to_client) {
  rt::ScopedLock<rt::Mutex> lock(&forwarding_mutex_);
  const iovec iovecs[] = {{&conn_to_client, sizeof(conn_to_client)},
                          {const_cast<ObjRPCRespHdr *>(&hdr), sizeof(hdr)}};
  BUG_ON(loader_conn_->WritevFull(std::span(iovecs)) < 0);
  BUG_ON(loader_conn_->WriteFull(payload, hdr.payload_size) < 0);

  barrier();
  if (--remaining_forwarding_cnts_ == 0) {
    loader_done_forwarding_.Signal();
  }
}

void Migrator::reserve_conns(uint32_t num, netaddr dest_server_addr) {
  conn_mgr_.reserve_conns(dest_server_addr, num);
}

} // namespace nu

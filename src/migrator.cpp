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

std::function<tcpconn_t *(netaddr)> MigratorConnManager::creator_ =
    [](netaddr server_addr) {
      tcpconn_t *c;
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      BUG_ON(tcp_dial(local_addr, server_addr, &c) != 0);
      return c;
    };

MigratorConnManager::MigratorConnManager()
    : ConnectionManager<netaddr>(creator_, kNumPerCoreCachedConns) {}

Migrator::~Migrator() { BUG(); }

inline void Migrator::handle_load(tcpconn_t *c) { load(c); }

void Migrator::handle_reserve_conns(tcpconn_t *c) {
  RPCReqReserveConns req;
  BUG_ON(!tcp_read_until(c, &req, sizeof(req)));
  reserve_conns(req.num, req.dest_server_addr);
}

void Migrator::handle_copy(tcpconn_t *c) {
  RPCReqCopy req;
  BUG_ON(!tcp_read_until(c, &req, sizeof(req)));
  BUG_ON(
      !tcp_read_until(c, reinterpret_cast<uint8_t *>(req.start_addr), req.len));
  req.wg->Done();
}

void Migrator::run_loop(uint16_t port) {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = port};
  BUG_ON(tcp_listen(addr, kTCPListenBackLog, &tcp_queue_) != 0);
  tcpconn_t *c;
  while (tcp_accept(tcp_queue_, &c) == 0) {
    rt::Thread([&, c] {
      while (true) {
        uint8_t type;
        auto ret = tcp_read_until(c, &type, sizeof(type));
        if (unlikely(!ret)) {
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
      BUG_ON(!tcp_write2_until(conn, &type, sizeof(type), &req, sizeof(req)));
      BUG_ON(!tcp_write_until(conn, reinterpret_cast<uint8_t *>(req.start_addr),
                              req.len));
      conn_mgr_.put_conn(dest_addr, conn);
    });
  }
  for (auto &thread : threads) {
    thread.Join();
  }
}

void Migrator::transmit_heap(tcpconn_t *c, netaddr dest_addr,
                             HeapHeader *heap_header) {
  uint8_t type = LOAD;
  BUG_ON(!tcp_write_until(c, &type, sizeof(type)));
  int obj_ref_cnt = heap_header->ref_cnt;
  BUG_ON(!tcp_write2_until(c, &heap_header, sizeof(heap_header), &obj_ref_cnt,
                           sizeof(obj_ref_cnt)));
  const auto &slab = heap_header->slab;
  auto start_addr = reinterpret_cast<uint64_t>(&slab);
  auto len = (reinterpret_cast<intptr_t>(slab.get_base()) -
              reinterpret_cast<intptr_t>(&slab)) +
             slab.get_usage();
  uint8_t ack;
  rt::WaitGroup *wg;
  BUG_ON(!tcp_read2_until(c, &ack, sizeof(ack), &wg, sizeof(wg)));
  parallel_transmit_heap(dest_addr, wg, start_addr, len);
}

void Migrator::transmit_mutexes(tcpconn_t *c, HeapHeader *heap_header,
                                std::unordered_set<thread_t *> *mutex_threads) {
  auto mutexes = heap_header->mutexes->all_keys();
  size_t num_mutexes = mutexes.size();
  BUG_ON(!tcp_write2_until(c, &num_mutexes, sizeof(num_mutexes), mutexes.data(),
                           num_mutexes * sizeof(Mutex *)));

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
    BUG_ON(!tcp_write_until(c, &num_threads, sizeof(num_threads)));
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
  }
}

void Migrator::transmit_condvars(
    tcpconn_t *c, HeapHeader *heap_header,
    std::unordered_set<thread_t *> *condvar_threads) {
  auto condvars = heap_header->condvars->all_keys();
  size_t num_condvars = condvars.size();
  BUG_ON(!tcp_write2_until(c, &num_condvars, sizeof(num_condvars),
                           condvars.data(), num_condvars * sizeof(CondVar *)));

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
    BUG_ON(!tcp_write_until(c, &num_threads, sizeof(num_threads)));
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
  }
}

void Migrator::transmit_time(tcpconn_t *c, HeapHeader *heap_header,
                             std::unordered_set<thread_t *> *time_threads) {
  uint64_t migrator_tsc = rdtscp(nullptr) - start_tsc;
  int64_t sum_tsc = migrator_tsc + heap_header->time->offset_tsc_;

  const auto &timer_entries_list = heap_header->time->entries_;
  size_t num_entries = timer_entries_list.size();
  BUG_ON(!tcp_write2_until(c, &sum_tsc, sizeof(sum_tsc), &num_entries,
                           sizeof(num_entries)));

  auto timer_entries_arr = std::make_unique<timer_entry *[]>(num_entries);
  std::copy(timer_entries_list.begin(), timer_entries_list.end(),
            timer_entries_arr.get());
  BUG_ON(!tcp_write_until(c, timer_entries_arr.get(),
                          sizeof(timer_entry *) * num_entries));

  for (size_t i = 0; i < num_entries; i++) {
    auto *entry = timer_entries_arr[i];
    auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
    time_threads->emplace(arg->th);
    transmit_one_thread(c, arg->th);
  }
}

void Migrator::transmit_one_thread(tcpconn_t *c, thread_t *thread) {
  size_t tf_size;
  auto *tf = thread_get_trap_frame(thread, &tf_size);
  BUG_ON(!tcp_write_until(c, tf, tf_size));
  void *stack_range[2];
  thread_get_obj_stack(thread, &stack_range[1], &stack_range[0]);
  BUG_ON(!tcp_write2_until(c, stack_range, sizeof(stack_range), stack_range[0],
                           reinterpret_cast<uintptr_t>(stack_range[1]) -
                               reinterpret_cast<uintptr_t>(stack_range[0]) +
                               1));
  thread_mark_migrated(thread);
}

void Migrator::transmit_threads(tcpconn_t *c,
                                const std::vector<thread_t *> &threads) {
  uint64_t num_threads = threads.size();
  BUG_ON(!tcp_write_until(c, &num_threads, sizeof(num_threads)));
  for (auto thread : threads) {
    transmit_one_thread(c, thread);
  }
}

void Migrator::forward_to_client(uint32_t num_rpcs,
                                 tcpconn_t *conn_to_new_server) {
  for (size_t i = 0; i < num_rpcs; i++) {
    tcpconn_t *conn_to_client;
    ObjRPCRespHdr hdr;
    BUG_ON(!tcp_read2_until(conn_to_new_server, &conn_to_client,
                            sizeof(conn_to_client), &hdr, sizeof(hdr)));
    auto payload = std::make_unique<uint8_t[]>(hdr.payload_size);
    BUG_ON(
        !tcp_read_until(conn_to_new_server, payload.get(), hdr.payload_size));
    BUG_ON(!tcp_write2_until(conn_to_client, &hdr, sizeof(hdr), payload.get(),
                             hdr.payload_size));
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

void *Migrator::load_heap(tcpconn_t *c, rt::Mutex *loader_mutex) {
  HeapHeader *heap_header;
  int obj_ref_cnt;
  if (!tcp_read2_until(c, &heap_header, sizeof(heap_header), &obj_ref_cnt,
                       sizeof(obj_ref_cnt))) {
    return nullptr;
  }
  // Only allows one loading at a time.
  loader_mutex->Lock();
  Runtime::heap_manager->mmap(heap_header);
  uint8_t ack;
  rt::WaitGroup wg(kTransmitHeapNumThreads);
  auto *wg_p = &wg;
  BUG_ON(!tcp_write2_until(c, &ack, sizeof(ack), &wg_p, sizeof(wg_p)));
  Runtime::heap_manager->setup(heap_header);
  heap_header->ref_cnt = obj_ref_cnt;
  wg.Wait();

  return heap_header;
}

thread_t *Migrator::load_one_thread(tcpconn_t *c, HeapHeader *heap_header) {
  auto tlsvar = reinterpret_cast<uint64_t>(&heap_header->slab);

  size_t tf_size;
  thread_get_trap_frame(thread_self(), &tf_size);
  auto tf = std::make_unique<uint8_t[]>(tf_size);
  BUG_ON(!tcp_read_until(c, tf.get(), tf_size));
  void *stack_range[2];
  BUG_ON(!tcp_read_until(c, stack_range, sizeof(stack_range)));
  BUG_ON(!tcp_read_until(c, stack_range[0],
                         reinterpret_cast<uintptr_t>(stack_range[1]) -
                             reinterpret_cast<uintptr_t>(stack_range[0]) + 1));
  return create_migrated_thread(tf.get(), tlsvar);
}

void Migrator::load_mutexes(tcpconn_t *c, HeapHeader *heap_header) {
  size_t num_mutexes;
  BUG_ON(!tcp_read_until(c, &num_mutexes, sizeof(num_mutexes)));
  auto mutexes = std::make_unique<Mutex *[]>(num_mutexes);
  BUG_ON(!tcp_read_until(c, mutexes.get(), num_mutexes * sizeof(Mutex *)));

  for (size_t i = 0; i < num_mutexes; i++) {
    auto mutex = mutexes[i];
    heap_header->mutexes->put(mutex);

    auto *waiters = mutex->get_waiters();
    list_head_init(waiters);
    size_t num_threads;
    BUG_ON(!tcp_read_until(c, &num_threads, sizeof(num_threads)));
    for (size_t j = 0; j < num_threads; j++) {
      auto *th = load_one_thread(c, heap_header);
      auto *th_link = reinterpret_cast<list_node *>(
          reinterpret_cast<uintptr_t>(th) + thread_link_offset);
      list_add_tail(waiters, th_link);
    }
  }
}

void Migrator::load_condvars(tcpconn_t *c, HeapHeader *heap_header) {
  size_t num_condvars;
  BUG_ON(!tcp_read_until(c, &num_condvars, sizeof(num_condvars)));
  auto condvars = std::make_unique<CondVar *[]>(num_condvars);
  BUG_ON(!tcp_read_until(c, condvars.get(), num_condvars * sizeof(CondVar *)));

  for (size_t i = 0; i < num_condvars; i++) {
    auto condvar = condvars[i];
    heap_header->condvars->put(condvar);

    auto *waiters = condvar->get_waiters();
    list_head_init(waiters);
    size_t num_threads;
    BUG_ON(!tcp_read_until(c, &num_threads, sizeof(num_threads)));
    for (size_t j = 0; j < num_threads; j++) {
      auto *th = load_one_thread(c, heap_header);
      auto *th_link = reinterpret_cast<list_node *>(
          reinterpret_cast<uintptr_t>(th) + thread_link_offset);
      list_add_tail(waiters, th_link);
    }
  }
}

void Migrator::load_time(tcpconn_t *c, HeapHeader *heap_header) {
  auto *time = heap_header->time.get();

  int64_t sum_tsc;
  size_t num_entries;
  BUG_ON(!tcp_read2_until(c, &sum_tsc, sizeof(sum_tsc), &num_entries,
                          sizeof(num_entries)));
  auto loader_tsc = rdtscp(nullptr) - start_tsc;
  time->offset_tsc_ = sum_tsc - loader_tsc;

  auto timer_entries = std::make_unique<timer_entry *[]>(num_entries);
  BUG_ON(!tcp_read_until(c, timer_entries.get(),
                         sizeof(timer_entry *) * num_entries));

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

void Migrator::load_threads(tcpconn_t *c, HeapHeader *heap_header) {
  uint64_t num_threads;
  BUG_ON(!tcp_read_until(c, &num_threads, sizeof(num_threads)));

  ACCESS_ONCE(remaining_forwarding_cnts_) = num_threads;

  for (uint64_t i = 0; i < num_threads; i++) {
    auto *th = load_one_thread(c, heap_header);
    heap_header->threads->put(th);
    thread_ready(th);
  }
}

void Migrator::load(tcpconn_t *c) {
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
                                          tcpconn_t *conn_to_client) {
  rt::ScopedLock<rt::Mutex> lock(&forwarding_mutex_);
  BUG_ON(!tcp_write2_until(loader_conn_, &conn_to_client,
                           sizeof(conn_to_client), &hdr, sizeof(hdr)));
  BUG_ON(!tcp_write_until(loader_conn_, payload, hdr.payload_size));

  barrier();
  if (--remaining_forwarding_cnts_ == 0) {
    loader_done_forwarding_.Signal();
  }
}

void Migrator::reserve_conns(uint32_t num, netaddr dest_server_addr) {
  conn_mgr_.reserve_conns(dest_server_addr, num);
}

} // namespace nu

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

#include "nu/cond_var.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/commons.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/migrator.hpp"
#include "nu/mutex.hpp"
#include "nu/obj_conn_mgr.hpp"
#include "nu/obj_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

constexpr static auto kMigrationDSCP = IPTOS_DSCP_CS0;

std::function<rt::TcpConn *(netaddr)> MigratorConnManager::creator_ =
    [](netaddr server_addr) {
      netaddr local_addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
      auto *c = rt::TcpConn::Dial(local_addr, server_addr, kMigrationDSCP);
      BUG_ON(!c);
      return c;
    };

MigratorConnManager::MigratorConnManager()
    : ConnectionManager<netaddr>(creator_, 0) {}

Migrator::~Migrator() { BUG(); }

void Migrator::handle_copy(rt::TcpConn *c) {
  RPCReqCopy req;
  BUG_ON(c->ReadFull(&req, sizeof(req)) <= 0);
  BUG_ON(c->ReadFull(reinterpret_cast<uint8_t *>(req.start_addr), req.len,
                     /* nt = */ true) <= 0);
  req.wg->Done();
}

inline void Migrator::handle_load(rt::TcpConn *c) { load(c); }

void Migrator::handle_reserve_conns(rt::TcpConn *c) {
  RPCReqReserveConns req;
  BUG_ON(c->ReadFull(&req, sizeof(req)) <= 0);
  reserve_conns(req.num, req.dest_server_addr);
}

void Migrator::handle_forward(rt::TcpConn *c) {
  rt::TcpConn *conn_to_client;
  ObjRPCRespHdr hdr;
  uint64_t stack_top;
  const iovec iovecs[] = {{&conn_to_client, sizeof(conn_to_client)},
                          {&stack_top, sizeof(stack_top)},
                          {&hdr, sizeof(hdr)}};
  BUG_ON(c->ReadvFull(std::span(iovecs)) <= 0);

  if (hdr.payload_size) {
    auto payload = std::make_unique<uint8_t[]>(hdr.payload_size);
    BUG_ON(c->ReadFull(payload.get(), hdr.payload_size) <= 0);
    const iovec iovecs[] = {{&hdr, sizeof(hdr)},
                            {payload.get(), hdr.payload_size}};
    BUG_ON(conn_to_client->WritevFull(std::span(iovecs)) < 0);
  } else {
    BUG_ON(conn_to_client->WriteFull(&hdr, sizeof(hdr)) < 0);
  }

  Runtime::stack_manager->put(reinterpret_cast<uint8_t *>(stack_top));

  rt::Thread(
      [&, conn_to_client] { Runtime::obj_server->handle_reqs(conn_to_client); })
      .Detach();
}

void Migrator::handle_unmap(rt::TcpConn *c) {
  uint64_t size;
  BUG_ON(c->ReadFull(&size, sizeof(size)) <= 0);
  auto num = size / sizeof(HeapHeader *);
  std::unique_ptr<HeapHeader *[]> heaps(new HeapHeader *[num]);
  BUG_ON(c->ReadFull(heaps.get(), size) <= 0);

  for (uint64_t i = 0; i < num; i++) {
    Runtime::heap_manager->deallocate(heaps[i]);
  }
}

void Migrator::run_loop() {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = kMigratorServerPort};
  auto tcp_queue =
      rt::TcpQueue::Listen(addr, kTCPListenBackLog, kMigrationDSCP);
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
        case COPY:
          handle_copy(c);
          break;
        case MIGRATE:
          handle_load(c);
          break;
        case RESERVE_CONNS:
          handle_reserve_conns(c);
          break;
        case FORWARD:
          handle_forward(c);
          break;
        case UNMAP:
          handle_unmap(c);
          break;
        default:
          BUG();
        }
      }
      BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
    })
        .Detach();
  }
}

void Migrator::transmit_heap(rt::TcpConn *c, HeapHeader *heap_header) {
  [[maybe_unused]] uint64_t start_tsc, end_tsc;
  if constexpr (kEnableLogging) {
    start_tsc = rdtsc();
  }

  rt::WaitGroup *wg;
  BUG_ON(c->ReadFull(&wg, sizeof(wg)) <= 0);

  std::vector<rt::Thread> threads;
  threads.reserve(kTransmitHeapNumThreads);
  auto dest_addr = c->RemoteAddr();

  auto start_addr = reinterpret_cast<uint64_t>(&heap_header->slab);
  auto len =
      (reinterpret_cast<uint64_t>(heap_header->slab.get_base()) - start_addr) +
      heap_header->slab.get_usage();

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
                             req.len, /* nt = */ true) < 0);
      conn_mgr_.put_conn(dest_addr, conn);
    });
  }

  BUG_ON(c->WriteFull(&heap_header->ref_cnt, sizeof(heap_header->ref_cnt)) < 0);

  for (auto &thread : threads) {
    thread.Join();
  }

  if constexpr (kEnableLogging) {
    end_tsc = rdtsc();
    preempt_disable();
    std::cout << "Transmit heap: size = " << len
              << ", cycles = " << end_tsc - start_tsc << std::endl;
    preempt_enable();
  }
}

void Migrator::transmit_mutexes(rt::TcpConn *c, HeapHeader *heap_header,
                                std::unordered_set<thread_t *> *mutex_threads) {
  auto mutexes = heap_header->mutexes->all_keys();
  size_t num_mutexes = mutexes.size();

  if (num_mutexes) {
    const iovec iovecs[] = {{&num_mutexes, sizeof(num_mutexes)},
                            {mutexes.data(), num_mutexes * sizeof(Mutex *)}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ true) < 0);
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
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ true) < 0);
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

  if (num_entries) {
    auto timer_entries_arr = std::make_unique<timer_entry *[]>(num_entries);
    std::copy(timer_entries_list.begin(), timer_entries_list.end(),
              timer_entries_arr.get());
    BUG_ON(c->WriteFull(timer_entries_arr.get(),
                        sizeof(timer_entry *) * num_entries,
                        /* nt = */ true) < 0);

    for (size_t i = 0; i < num_entries; i++) {
      auto *entry = timer_entries_arr[i];
      timer_cancel(entry);
      auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
      time_threads->emplace(arg->th);
      transmit_one_thread(c, arg->th);
    }
  }
}

void Migrator::transmit_one_thread(rt::TcpConn *c, thread_t *thread) {
  size_t tf_size;
  auto *tf = thread_get_trap_frame(thread, &tf_size);
  BUG_ON(c->WriteFull(tf, tf_size) < 0);

  auto stack_range = get_obj_stack_range(thread);
  auto stack_len = stack_range.end - stack_range.start;
  const iovec iovecs[] = {
      {&stack_range, sizeof(stack_range)},
      {reinterpret_cast<void *>(stack_range.start), stack_len}};
  BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ true) < 0);

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

void Migrator::transmit_stack_cluster_mmap_task(rt::TcpConn *c) {
  auto stack_cluster = Runtime::stack_manager->get_range();
  BUG_ON(c->WriteFull(&stack_cluster, sizeof(stack_cluster)) < 0);
}

std::vector<HeapMmapPopulateRange>
Migrator::transmit_heap_mmap_populate_ranges(rt::TcpConn *c,
                                             const std::list<void *> &heaps) {
  std::vector<HeapMmapPopulateRange> populate_ranges;
  populate_ranges.reserve(heaps.size());

  for (auto heap : heaps) {
    if (unlikely(!Runtime::heap_manager->contains(heap))) {
      continue;
    }
    auto *heap_header = reinterpret_cast<HeapHeader *>(heap);
    auto &slab = heap_header->slab;
    uint64_t len = reinterpret_cast<uint64_t>(slab.get_base()) +
                   slab.get_usage() - reinterpret_cast<uint64_t>(heap_header);
    HeapMmapPopulateRange range{heap_header, len};
    populate_ranges.push_back(range);
  }

  uint64_t size = populate_ranges.size() * sizeof(HeapMmapPopulateRange);
  if (size) {
    const iovec iovecs[] = {{&size, sizeof(size)},
                            {populate_ranges.data(), size}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ true) < 0);
  } else {
    BUG_ON(c->WriteFull(&size, sizeof(size)) < 0);
  }

  return populate_ranges;
}

void Migrator::transmit(rt::TcpConn *c, HeapHeader *heap_header) {
  transmit_heap(c, heap_header);

  std::unordered_set<thread_t *> blocked_threads;
  transmit_mutexes(c, heap_header, &blocked_threads);
  transmit_condvars(c, heap_header, &blocked_threads);
  transmit_time(c, heap_header, &blocked_threads);

  auto all_threads = heap_header->threads->all_keys();
  std::vector<thread_t *> running_threads;
  for (auto thread : all_threads) {
    if (!blocked_threads.contains(thread)) {
      running_threads.push_back(thread);
    }
  }
  transmit_threads(c, running_threads);
}

bool Migrator::mark_migrating_threads(HeapHeader *heap_header) {
  if (!Runtime::heap_manager->remove(heap_header)) {
    return false;
  }
  ACCESS_ONCE(heap_header->migrating) = true;
  Runtime::heap_manager->rcu_writer_sync();
  auto all_threads = heap_header->threads->all_keys();
  for (auto thread : all_threads) {
    thread_mark_migrating(thread);
  }
  return true;
}

void Migrator::unmap_destructed_heaps(
    rt::TcpConn *c, std::vector<HeapHeader *> *destructed_heaps) {
  if (!destructed_heaps->empty()) {
    uint8_t type = UNMAP;
    uint64_t size = destructed_heaps->size() * sizeof(HeapHeader *);
    const iovec iovecs[] = {{&type, sizeof(type)},
                            {&size, sizeof(size)},
                            {destructed_heaps->data(), size}};
    BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  }
}

void Migrator::migrate(Resource pressure, std::list<void *> heaps) {
  auto optional_dest_addr =
      Runtime::controller_client->get_migration_dest(pressure);
  BUG_ON(!optional_dest_addr);
  auto dest_addr = *optional_dest_addr;
  auto conn = conn_mgr_.get_conn(dest_addr);

  uint8_t type = MIGRATE;
  BUG_ON(conn->WriteFull(&type, sizeof(type)) < 0);
  transmit_stack_cluster_mmap_task(conn);
  auto heap_populate_ranges = transmit_heap_mmap_populate_ranges(conn, heaps);

  std::vector<HeapHeader *> migrated_heaps;
  std::vector<HeapHeader *> destructed_heaps;
  migrated_heaps.reserve(heap_populate_ranges.size());
  for (auto [heap_header, _] : heap_populate_ranges) {
    if (unlikely(!mark_migrating_threads(heap_header))) {
      destructed_heaps.push_back(heap_header);
      continue;
    }
    migrated_heaps.push_back(heap_header);
    pause_migrating_threads();
    transmit(conn, heap_header);
    gc_migrated_threads();
  }

  for (auto *heap_header : migrated_heaps) {
    Runtime::heap_manager->deallocate(heap_header);
  }

  unmap_destructed_heaps(conn, &destructed_heaps);

  conn_mgr_.put_conn(dest_addr, conn);
}

void Migrator::load_heap(rt::TcpConn *c, HeapMmapPopulateTask *task) {
  [[maybe_unused]] uint64_t t0, t1, t2;
  if constexpr (kEnableLogging) {
    t0 = rdtsc();
  }

  task->mu->Lock();
  while (unlikely(!ACCESS_ONCE(task->mmapped))) {
    task->cv->Wait(task->mu.get());
  }
  task->mu->Unlock();

  if constexpr (kEnableLogging) {
    t1 = rdtsc();
  }
  rt::WaitGroup wg(kTransmitHeapNumThreads);
  auto *wg_p = &wg;
  BUG_ON(c->WriteFull(&wg_p, sizeof(wg_p)) < 0);

  auto *heap_header = task->range.heap_header;
  BUG_ON(c->ReadFull(&heap_header->ref_cnt, sizeof(heap_header->ref_cnt)) <= 0);

  wg.Wait();

  if constexpr (kEnableLogging) {
    t2 = rdtsc();
    preempt_disable();
    std::cout << "Load heap: mmap cycles = " << t1 - t0
              << ", tcp cycles = " << t2 - t1 << std::endl;
    preempt_enable();
  }
}

thread_t *Migrator::load_one_thread(rt::TcpConn *c, HeapHeader *heap_header) {
  auto tlsvar = reinterpret_cast<uint64_t>(&heap_header->slab);

  size_t tf_size;
  thread_get_trap_frame(thread_self(), &tf_size);
  auto tf = std::make_unique<uint8_t[]>(tf_size);
  BUG_ON(c->ReadFull(tf.get(), tf_size) <= 0);

  VAddrRange stack_range;
  BUG_ON(c->ReadFull(&stack_range, sizeof(stack_range)) <= 0);
  auto stack_len = stack_range.end - stack_range.start;
  BUG_ON(c->ReadFull(reinterpret_cast<void *>(stack_range.start), stack_len,
                     /* nt = */ true) <= 0);

  return create_migrated_thread(tf.get(), tlsvar);
}

void Migrator::load_mutexes(rt::TcpConn *c, HeapHeader *heap_header) {
  size_t num_mutexes;
  BUG_ON(c->ReadFull(&num_mutexes, sizeof(num_mutexes)) <= 0);

  if (num_mutexes) {
    auto mutexes = std::make_unique<Mutex *[]>(num_mutexes);
    BUG_ON(c->ReadFull(mutexes.get(), num_mutexes * sizeof(Mutex *),
                       /* nt = */ true) <= 0);

    for (size_t i = 0; i < num_mutexes; i++) {
      auto mutex = mutexes[i];
      heap_header->mutexes->put(mutex);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);
      heap_header->forward_wg.Add(num_threads);

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
    BUG_ON(c->ReadFull(condvars.get(), num_condvars * sizeof(CondVar *),
                       /* nt = */ true) <= 0);

    for (size_t i = 0; i < num_condvars; i++) {
      auto condvar = condvars[i];
      heap_header->condvars->put(condvar);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);
      heap_header->forward_wg.Add(num_threads);

      auto *waiters = condvar->get_waiters();
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

void Migrator::load_time(rt::TcpConn *c, HeapHeader *heap_header) {
  auto *time = heap_header->time.get();

  int64_t sum_tsc;
  size_t num_entries;
  const iovec iovecs[] = {{&sum_tsc, sizeof(sum_tsc)},
                          {&num_entries, sizeof(num_entries)}};
  BUG_ON(c->ReadvFull(std::span(iovecs)) <= 0);
  heap_header->forward_wg.Add(num_entries);

  auto loader_tsc = rdtscp(nullptr) - start_tsc;
  time->offset_tsc_ = sum_tsc - loader_tsc;

  if (num_entries) {
    auto timer_entries = std::make_unique<timer_entry *[]>(num_entries);
    BUG_ON(c->ReadFull(timer_entries.get(), sizeof(timer_entry *) * num_entries,
                       /* nt = */ true) <= 0);

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
  heap_header->forward_wg.Add(num_threads);

  for (uint64_t i = 0; i < num_threads; i++) {
    auto *th = load_one_thread(c, heap_header);
    heap_header->threads->put(th);
    thread_ready(th);
  }
}

VAddrRange Migrator::load_stack_cluster_mmap_task(rt::TcpConn *c) {
  VAddrRange stack_cluster;
  BUG_ON(c->ReadFull(&stack_cluster, sizeof(stack_cluster)) <= 0);
  StackManager::mmap(stack_cluster);
  return stack_cluster;
}

std::vector<HeapMmapPopulateRange>
Migrator::load_heap_mmap_populate_ranges(rt::TcpConn *c) {
  std::vector<HeapMmapPopulateRange> populate_ranges;
  uint64_t size;

  BUG_ON(c->ReadFull(&size, sizeof(size)) <= 0);

  if (size) {
    populate_ranges.resize(size / sizeof(HeapMmapPopulateRange));
    BUG_ON(c->ReadFull(&populate_ranges[0], size, /* nt = */ true) <= 0);
  }

  return populate_ranges;
}

rt::Thread Migrator::do_heap_mmap_populate(
    uint32_t old_server_ip,
    const std::vector<HeapMmapPopulateRange> &populate_ranges,
    std::vector<HeapMmapPopulateTask> *populate_tasks) {
  populate_tasks->reserve(populate_ranges.size());
  for (size_t i = 0; i < populate_ranges.size(); i++) {
    populate_tasks->emplace_back(populate_ranges[i]);
  }
  return rt::Thread([populate_tasks, old_server_ip] {
    for (auto &task : *populate_tasks) {
      Runtime::heap_manager->mmap_populate(task.range.heap_header,
                                           task.range.len);
      Runtime::heap_manager->setup(task.range.heap_header,
                                   /* migratable = */ false,
                                   /* from_migration = */ true);
      task.range.heap_header->old_server_ip = old_server_ip;
      task.mu->Lock();
      task.mmapped = true;
      task.cv->SignalAll();
      task.mu->Unlock();
    }
  });
}

void Migrator::load(rt::TcpConn *c) {
  auto stack_cluster = load_stack_cluster_mmap_task(c);
  auto populate_ranges = load_heap_mmap_populate_ranges(c);

  std::vector<HeapMmapPopulateTask> heap_mmap_populate_tasks;
  auto mmap_thread = do_heap_mmap_populate(c->RemoteAddr().ip, populate_ranges,
                                           &heap_mmap_populate_tasks);
  rt::WaitGroup *stack_wg;
  bool just_constructed =
      !StackManager::get_waitgroup(&stack_wg, stack_cluster);
  stack_wg->Add(heap_mmap_populate_tasks.size());

  for (auto &task : heap_mmap_populate_tasks) {
    auto *heap_header = task.range.heap_header;

    load_heap(c, &task);

    load_mutexes(c, heap_header);
    load_condvars(c, heap_header);
    load_time(c, heap_header);

    Runtime::heap_manager->insert(heap_header);

    rt::Thread([heap_header] {
      Runtime::controller_client->update_location(
          to_obj_id(heap_header), Runtime::obj_server->get_addr());
    }).Detach();

    load_threads(c, heap_header);

    rt::Thread([heap_header, stack_cluster, stack_wg] {
      heap_header->forward_wg.Wait();
      ACCESS_ONCE(heap_header->migratable) = true;
      stack_wg->Done();
    }).Detach();
  }

  if (just_constructed) {
    rt::Thread([stack_wg, stack_cluster] {
      stack_wg->Wait();
      StackManager::munmap(stack_cluster);
    }).Detach();
  }

  mmap_thread.Join();
}

void Migrator::forward_to_original_server(rt::TcpConn *conn_to_client,
                                          uint64_t stack_top,
                                          const ObjRPCRespHdr &hdr,
                                          const void *payload) {
  uint8_t type = FORWARD;
  auto *heap_header = Runtime::get_current_obj_heap_header();
  netaddr old_server_addr = {.ip = heap_header->old_server_ip,
                             .port = kMigratorServerPort};
  auto conn = conn_mgr_.get_conn(old_server_addr);

  const iovec iovecs[] = {{&type, sizeof(type)},
                          {&conn_to_client, sizeof(conn_to_client)},
                          {&stack_top, sizeof(stack_top)},
                          {const_cast<ObjRPCRespHdr *>(&hdr), sizeof(hdr)}};
  BUG_ON(conn->WritevFull(std::span(iovecs)) < 0);
  if (hdr.payload_size) {
    BUG_ON(conn->WriteFull(payload, hdr.payload_size) < 0);
  }
  heap_header->forward_wg.Done();

  conn_mgr_.put_conn(old_server_addr, conn);
}

void Migrator::reserve_conns(uint32_t num, netaddr dest_server_addr) {
  conn_mgr_.reserve_conns(dest_server_addr, num);
}

} // namespace nu

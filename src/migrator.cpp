#include <algorithm>
#include <cstddef>
#include <cstring>
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

#include "nu/commons.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/migrator.hpp"
#include "nu/obj_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

constexpr static auto kMigrationDSCP = IPTOS_DSCP_CS0;

MigratorConn::MigratorConn(rt::TcpConn *tcp_conn, netaddr addr,
                           MigratorConnManager *manager)
    : tcp_conn_(tcp_conn), addr_(addr), manager_(manager) {}

MigratorConn::~MigratorConn() {
  if (tcp_conn_) {
    manager_->put(addr_, tcp_conn_);
  }
}

MigratorConn::MigratorConn(MigratorConn &&o)
    : tcp_conn_(o.tcp_conn_), addr_(o.addr_), manager_(o.manager_) {
  o.tcp_conn_ = nullptr;
}

MigratorConn &MigratorConn::operator=(MigratorConn &&o) {
  tcp_conn_ = o.tcp_conn_;
  addr_ = o.addr_;
  manager_ = o.manager_;
  o.tcp_conn_ = nullptr;
  return *this;
}

rt::TcpConn *MigratorConn::get_tcp_conn() { return tcp_conn_; }

MigratorConnManager::~MigratorConnManager() {
  for (auto &[_, pool] : pool_map_) {
    while (!pool.empty()) {
      auto *tcp_conn = pool.top();
      pool.pop();
      BUG_ON(tcp_conn->Shutdown(SHUT_RDWR) < 0);
      std::unique_ptr<rt::TcpConn> gc(tcp_conn);
    }
  }
}

MigratorConn MigratorConnManager::get(netaddr raddr) {
  rt::MutexGuard guard(&mutex_);
  auto &pool = pool_map_[raddr];
  if (likely(!pool.empty())) {
    auto *tcp_conn = pool.top();
    pool.pop();
    return MigratorConn(tcp_conn, raddr, this);
  }
  netaddr laddr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  auto *tcp_conn = rt::TcpConn::Dial(laddr, raddr, kMigrationDSCP);
  BUG_ON(!tcp_conn);
  return MigratorConn(tcp_conn, raddr, this);
}

void MigratorConnManager::put(netaddr addr, rt::TcpConn *tcp_conn) {
  rt::MutexGuard guard(&mutex_);
  pool_map_[addr].push(tcp_conn);
}

Migrator::~Migrator() { BUG(); }

void Migrator::handle_copy(rt::TcpConn *c) {
  uint64_t start_addr, len;
  rt::WaitGroup *wg;
  const iovec iovecs[] = {{&start_addr, sizeof(start_addr)},
                          {&len, sizeof(len)},
                          {&wg, sizeof(wg)}};
  BUG_ON(c->ReadvFull(std::span(iovecs)) <= 0);
  BUG_ON(c->ReadFull(reinterpret_cast<uint8_t *>(start_addr), len,
                     /* nt = */ true) <= 0);
  wg->Done();
}

inline void Migrator::handle_load(rt::TcpConn *c) { load(c); }

void Migrator::handle_forward(rt::TcpConn *c) {
  RPCReturnCode rc;
  RPCReturner returner;
  uint64_t stack_top;
  uint64_t payload_len;
  const iovec iovecs[] = {{&rc, sizeof(rc)},
                          {&returner, sizeof(returner)},
                          {&stack_top, sizeof(stack_top)},
                          {&payload_len, sizeof(payload_len)}};
  BUG_ON(c->ReadvFull(std::span(iovecs)) <= 0);

  if (payload_len) {
    auto payload_buf = std::make_unique_for_overwrite<std::byte[]>(payload_len);
    BUG_ON(c->ReadFull(payload_buf.get(), payload_len) <= 0);
    auto span = std::span(payload_buf.get(), payload_len);
    returner.Return(rc, span, [payload_buf = std::move(payload_buf)] {});
  } else {
    returner.Return(rc);
  }
  Runtime::stack_manager->put(reinterpret_cast<uint8_t *>(stack_top));
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
        case kCopy:
          handle_copy(c);
          break;
        case kMigrate:
          handle_load(c);
          break;
        case kForward:
          handle_forward(c);
          break;
        case kUnmap:
          handle_unmap(c);
          break;
        default:
          BUG();
        }
      }
      BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
    }).Detach();
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
      auto migrator_conn = migrator_conn_mgr_.get(dest_addr);
      auto *conn = migrator_conn.get_tcp_conn();
      uint8_t type = kCopy;
      auto per_thread_len = (len - 1) / kTransmitHeapNumThreads + 1;
      uint64_t req_start_addr = start_addr + tid * per_thread_len;
      uint64_t req_len = (tid != kTransmitHeapNumThreads - 1)
                             ? per_thread_len
                             : start_addr + len - req_start_addr;
      const iovec iovecs[] = {{&type, sizeof(type)},
                              {&req_start_addr, sizeof(req_start_addr)},
                              {&req_len, sizeof(req_len)},
                              {&wg, sizeof(wg)}};
      BUG_ON(conn->WritevFull(std::span(iovecs)) < 0);
      BUG_ON(conn->WriteFull(reinterpret_cast<uint8_t *>(req_start_addr),
                             req_len, /* nt = */ true) < 0);
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

void Migrator::transmit_mutexes(rt::TcpConn *c, std::vector<Mutex *> mutexes) {
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
    }
    size_t num_threads = ths.size();
    BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads)) < 0);
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
  }
}

void Migrator::transmit_condvars(rt::TcpConn *c,
                                 std::vector<CondVar *> condvars) {
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
    }
    size_t num_threads = ths.size();
    BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads)) < 0);
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
  }
}

void Migrator::transmit_time(rt::TcpConn *c, Time *time) {
  uint64_t migrator_tsc = rdtscp(nullptr) - start_tsc;
  int64_t sum_tsc = migrator_tsc + time->offset_tsc_;

  const auto &timer_entries_list = time->entries_;
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

void Migrator::transmit_heap_mmap_populate_ranges(
    rt::TcpConn *c, const std::vector<HeapRange> &heaps) {
  uint64_t size = heaps.size() * sizeof(HeapRange);
  if (size) {
    const iovec iovecs[] = {{&size, sizeof(size)},
                            {const_cast<HeapRange *>(heaps.data()), size}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ true) < 0);
  } else {
    BUG_ON(c->WriteFull(&size, sizeof(size)) < 0);
  }
}

void Migrator::transmit(rt::TcpConn *c, HeapHeader *heap_header) {
  transmit_heap(c, heap_header);

  auto all_threads = heap_header->threads->all_keys();
  std::vector<thread_t *> ready_threads;
  std::unordered_set<Mutex *> mutexes;
  std::unordered_set<CondVar *> condvars;

  for (auto *thread : all_threads) {
    WaiterInfo waiter_info;
    bool ready;
    get_waiter_info_and_ready(thread, &waiter_info.raw, &ready);
    // Cannot simply use waiter_info.type == WaiterType::None here, since it's
    // vulnerable to race conditions.
    if (likely(ready)) {
      ready_threads.push_back(thread);
    } else {
      if (waiter_info.type == WaiterType::kMutex) {
        mutexes.emplace(reinterpret_cast<Mutex *>(waiter_info.addr));
      } else if (waiter_info.type == WaiterType::kCondVar) {
        condvars.emplace(reinterpret_cast<CondVar *>(waiter_info.addr));
      } else {
        BUG_ON(waiter_info.type != WaiterType::kTimer);
      }
    }
  }

  transmit_mutexes(c, std::vector<Mutex *>(mutexes.begin(), mutexes.end()));
  transmit_condvars(c,
                    std::vector<CondVar *>(condvars.begin(), condvars.end()));
  transmit_time(c, heap_header->time.get());
  transmit_threads(c, ready_threads);;
}

bool Migrator::mark_migrating_threads(HeapHeader *heap_header) {
  if (unlikely(!Runtime::heap_manager->remove(heap_header))) {
    return false;
  }
  heap_header->rcu_lock.writer_sync();
  auto all_threads = heap_header->threads->all_keys();
  for (auto thread : all_threads) {
    thread_mark_migrating(thread);
  }
  return true;
}

void Migrator::unmap_destructed_heaps(
    rt::TcpConn *c, std::vector<HeapHeader *> *destructed_heaps) {
  if (!destructed_heaps->empty()) {
    uint8_t type = kUnmap;
    uint64_t num_heaps = destructed_heaps->size();
    VAddrRange stack_cluster = Runtime::stack_manager->get_range();
    const iovec iovecs[] = {
        {&type, sizeof(type)},
        {&num_heaps, sizeof(num_heaps)},
        {&stack_cluster, sizeof(stack_cluster)},
        {destructed_heaps->data(), num_heaps * sizeof(HeapHeader *)}};
    BUG_ON(c->WritevFull(std::span(iovecs)) < 0);
  }
}

void Migrator::handle_unmap(rt::TcpConn *c) {
  uint64_t num_heaps;
  VAddrRange stack_cluster;
  const iovec iovecs[] = {{&num_heaps, sizeof(num_heaps)},
                          {&stack_cluster, sizeof(stack_cluster)}};
  BUG_ON(c->ReadvFull(std::span(iovecs)) <= 0);
  HeapHeader *heaps[num_heaps];
  BUG_ON(c->ReadFull(heaps, sizeof(heaps)) <= 0);

  Runtime::stack_manager->add_ref_cnt(stack_cluster, -num_heaps);
  for (uint64_t i = 0; i < num_heaps; i++) {
    Runtime::heap_manager->deallocate(heaps[i]);
  }
}

void Migrator::migrate(Resource pressure, std::vector<HeapRange> heaps) {
  auto optional_dest_addr =
      Runtime::controller_client->get_migration_dest(pressure);
  BUG_ON(!optional_dest_addr);
  auto dest_addr = *optional_dest_addr;
  auto migration_conn = migrator_conn_mgr_.get(dest_addr);
  auto *conn = migration_conn.get_tcp_conn();

  uint8_t type = kMigrate;
  BUG_ON(conn->WriteFull(&type, sizeof(type)) < 0);
  transmit_stack_cluster_mmap_task(conn);
  transmit_heap_mmap_populate_ranges(conn, heaps);

  std::vector<HeapHeader *> migrated_heaps;
  std::vector<HeapHeader *> destructed_heaps;
  migrated_heaps.reserve(heaps.size());
  for (auto [heap_header, _] : heaps) {
    if (unlikely(!mark_migrating_threads(heap_header))) {
      destructed_heaps.push_back(heap_header);
      continue;
    }
    migrated_heaps.push_back(heap_header);
    pause_migrating_threads();
    transmit(conn, heap_header);
    SlabAllocator::deregister_slab_by_id(to_u16(heap_header));
    gc_migrated_threads();
  }

  for (auto *heap_header : migrated_heaps) {
    Runtime::heap_manager->deallocate(heap_header);
  }

  unmap_destructed_heaps(conn, &destructed_heaps);
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

  auto *slab = &heap_header->slab;
  nu::SlabAllocator::register_slab_by_id(slab, slab->get_id());

  if constexpr (kEnableLogging) {
    t2 = rdtsc();
    preempt_disable();
    std::cout << "Load heap: mmap cycles = " << t1 - t0
              << ", tcp cycles = " << t2 - t1 << std::endl;
    preempt_enable();
  }
}

thread_t *Migrator::load_one_thread(rt::TcpConn *c, HeapHeader *heap_header) {
  auto *obj_heap = &heap_header->slab;

  size_t tf_size;
  thread_get_trap_frame(thread_self(), &tf_size);
  auto tf = std::make_unique<uint8_t[]>(tf_size);
  BUG_ON(c->ReadFull(tf.get(), tf_size) <= 0);

  VAddrRange stack_range;
  BUG_ON(c->ReadFull(&stack_range, sizeof(stack_range)) <= 0);
  auto stack_len = stack_range.end - stack_range.start;
  BUG_ON(c->ReadFull(reinterpret_cast<void *>(stack_range.start), stack_len,
                     /* nt = */ true) <= 0);
  auto *th = create_migrated_thread(tf.get(), obj_heap);
  heap_header->threads->put(th);
  return th;
}

void Migrator::load_mutexes(rt::TcpConn *c, HeapHeader *heap_header) {
  size_t num_mutexes;
  BUG_ON(c->ReadFull(&num_mutexes, sizeof(num_mutexes)) <= 0);

  if (num_mutexes) {
    auto mutexes = std::make_unique<Mutex *[]>(num_mutexes);
    BUG_ON(c->ReadFull(mutexes.get(), num_mutexes * sizeof(Mutex *),
                       /* nt = */ true) <= 0);

    for (size_t i = 0; i < num_mutexes; i++) {
      auto *mutex = mutexes[i];

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);
      heap_header->forward_wg.Add(num_threads);

      auto *waiters = mutex->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto *th = load_one_thread(c, heap_header);
        WaiterInfo waiter_info;
        waiter_info.type = WaiterType::kMutex;
        waiter_info.addr = reinterpret_cast<uint64_t>(mutex);
        set_waiter_info(th, waiter_info.raw);
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
      auto *condvar = condvars[i];

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads)) <= 0);
      heap_header->forward_wg.Add(num_threads);

      auto *waiters = condvar->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto *th = load_one_thread(c, heap_header);
        WaiterInfo waiter_info;
        waiter_info.type = WaiterType::kCondVar;
        waiter_info.addr = reinterpret_cast<uint64_t>(condvar);
        set_waiter_info(th, waiter_info.raw);
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
      {
        rt::SpinGuard guard(&time->spin_);
        time->entries_.push_back(entry);
        arg->iter = --time->entries_.end();
      }
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
    thread_ready(th);
  }
}

VAddrRange Migrator::load_stack_cluster_mmap_task(rt::TcpConn *c) {
  VAddrRange stack_cluster;
  BUG_ON(c->ReadFull(&stack_cluster, sizeof(stack_cluster)) <= 0);
  return stack_cluster;
}

std::vector<HeapRange>
Migrator::load_heap_mmap_populate_ranges(rt::TcpConn *c) {
  std::vector<HeapRange> populate_ranges;
  uint64_t size;

  BUG_ON(c->ReadFull(&size, sizeof(size)) <= 0);

  if (size) {
    populate_ranges.resize(size / sizeof(HeapRange));
    BUG_ON(c->ReadFull(&populate_ranges[0], size, /* nt = */ true) <= 0);
  }

  return populate_ranges;
}

rt::Thread Migrator::do_heap_mmap_populate(
    uint32_t old_server_ip, const std::vector<HeapRange> &populate_ranges,
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
      rt::MutexGuard guard(task.mu.get());
      task.mmapped = true;
      task.cv->SignalAll();
    }
  });
}

void Migrator::load(rt::TcpConn *c) {
  auto stack_cluster = load_stack_cluster_mmap_task(c);
  auto populate_ranges = load_heap_mmap_populate_ranges(c);

  std::vector<HeapMmapPopulateTask> heap_mmap_populate_tasks;
  auto mmap_thread = do_heap_mmap_populate(c->RemoteAddr().ip, populate_ranges,
                                           &heap_mmap_populate_tasks);
  Runtime::stack_manager->add_ref_cnt(stack_cluster,
                                      heap_mmap_populate_tasks.size());

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

    rt::Thread([heap_header, stack_cluster] {
      heap_header->forward_wg.Wait();
      ACCESS_ONCE(heap_header->migratable) = true;
      Runtime::stack_manager->add_ref_cnt(stack_cluster, -1);
    }).Detach();
  }

  mmap_thread.Join();
}

void Migrator::reserve_conns(netaddr dest_server_addr) {
  std::vector<MigratorConn> migrator_conns;
  migrator_conns.reserve(kDefaultNumReservedConns);
  for (uint32_t i = 0; i < kDefaultNumReservedConns; i++) {
    migrator_conns.emplace_back(migrator_conn_mgr_.get(dest_server_addr));
  }
}

void Migrator::forward_to_original_server(RPCReturnCode rc,
                                          RPCReturner *returner,
                                          uint64_t payload_len,
                                          const void *payload) {
  uint8_t type = kForward;
  auto *heap_header = Runtime::get_current_obj_heap_header();
  netaddr old_server_addr = {.ip = heap_header->old_server_ip,
                             .port = kMigratorServerPort};
  {
    RuntimeHeapGuard guard;
    auto migrator_conn = migrator_conn_mgr_.get(old_server_addr);
    auto *conn = migrator_conn.get_tcp_conn();
    uint64_t stack_top = get_obj_stack_range(thread_self()).end;
    const iovec iovecs[] = {{&type, sizeof(type)},
                            {&rc, sizeof(rc)},
                            {returner, sizeof(*returner)},
                            {&stack_top, sizeof(stack_top)},
                            {&payload_len, sizeof(payload_len)}};
    BUG_ON(conn->WritevFull(std::span(iovecs)) < 0);
    if (payload_len) {
      BUG_ON(conn->WriteFull(payload, payload_len) < 0);
    }
  }
  heap_header->forward_wg.Done();
}

} // namespace nu

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <span>
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
#include "nu/pressure_handler.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/thread.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

constexpr static auto kMigrationDSCP = IPTOS_DSCP_CS0;

MigratorConn::MigratorConn() : tcp_conn_(nullptr), ip_(0), manager_(nullptr) {}

MigratorConn::MigratorConn(rt::TcpConn *tcp_conn, uint32_t ip,
                           MigratorConnManager *manager)
    : tcp_conn_(tcp_conn), ip_(ip), manager_(manager) {}

MigratorConn::~MigratorConn() { release(); }

void MigratorConn::release() {
  if (tcp_conn_) {
    manager_->put(ip_, tcp_conn_);
  }
}

MigratorConn::MigratorConn(MigratorConn &&o)
    : tcp_conn_(o.tcp_conn_), ip_(o.ip_), manager_(o.manager_) {
  o.tcp_conn_ = nullptr;
}

MigratorConn &MigratorConn::operator=(MigratorConn &&o) {
  tcp_conn_ = o.tcp_conn_;
  ip_ = o.ip_;
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

MigratorConn MigratorConnManager::get(uint32_t ip) {
  rt::SpinGuard guard(&spin_);
  auto &pool = pool_map_[ip];
  if (likely(!pool.empty())) {
    auto *tcp_conn = pool.top();
    pool.pop();
    return MigratorConn(tcp_conn, ip, this);
  }
  netaddr laddr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
  netaddr raddr = {.ip = ip, .port = Migrator::kPort};
  auto *tcp_conn =
      rt::TcpConn::Dial(laddr, raddr, kMigrationDSCP, /* poll = */ true);
  BUG_ON(!tcp_conn);
  return MigratorConn(tcp_conn, ip, this);
}

void MigratorConnManager::put(uint32_t ip, rt::TcpConn *tcp_conn) {
  rt::SpinGuard guard(&spin_);
  pool_map_[ip].push(tcp_conn);
}

Migrator::Migrator() { ever_migrated_ = false; }

Migrator::~Migrator() { BUG(); }

void Migrator::handle_copy_heap(rt::TcpConn *c) {
  uint64_t start_addr, len;
  const iovec iovecs[] = {{&start_addr, sizeof(start_addr)},
                          {&len, sizeof(len)}};
  BUG_ON(c->ReadvFull(std::span(iovecs), /* nt = */ false, /* poll = */ true) <=
         0);
  auto heap_base_addr = (start_addr & (~(kHeapSize - 1)));
  auto *heap_header = reinterpret_cast<HeapHeader *>(heap_base_addr);

  while (unlikely(rt::access_once(heap_header->status) != kMapped)) {
    unblock_and_relax();
  }

  BUG_ON(c->ReadFull(reinterpret_cast<uint8_t *>(start_addr), len,
                     /* nt = */ true, /* poll = */ true) <= 0);
  heap_header->pending_load_cnt--;
}

inline void Migrator::handle_load(rt::TcpConn *c) {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  load(c);
}

void Migrator::handle_register_callback(rt::TcpConn *c) {
  callback_conns_.insert(c);
}

void Migrator::handle_deregister_callback(rt::TcpConn *c) {
  BUG_ON(callback_conns_.erase(c) == 0);
}

void Migrator::run_background_loop() {
  netaddr addr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = kPort};
  tcp_queue_.reset(
      rt::TcpQueue::Listen(addr, kTCPListenBackLog, kMigrationDSCP));

  rt::Thread([&] {
    rt::TcpConn *c;
    while ((c = tcp_queue_->Accept())) {
      rt::Thread([&, c] {
        std::unique_ptr<rt::TcpConn> gc(c);

        bool poll = false;
        while (true) {
          uint8_t type;
          if (unlikely(c->ReadFull(&type, sizeof(type), /* nt = */ false,
                                   poll) <= 0)) {
            break;
          }

          switch (type) {
          case kCopyHeap:
            handle_copy_heap(c);
            break;
          case kMigrate:
            handle_load(c);
            break;
          case kUnmap:
            handle_unmap(c);
            break;
          case kEnablePoll:
            poll = true;
            preempt_disable();
            break;
          case kDisablePoll:
            poll = false;
            preempt_enable();
            break;
          case kRegisterCallBack:
            handle_register_callback(c);
            break;
          case kDeregisterCallBack:
            handle_deregister_callback(c);
            break;
          default:
            BUG();
          }
        }
        BUG_ON(c->Shutdown(SHUT_RDWR) < 0);
      }).Detach();
    }
  }).Detach();
}

void Migrator::transmit_heap(rt::TcpConn *c, HeapHeader *heap_header) {
  [[maybe_unused]] uint64_t t0, t1;
  if constexpr (kEnableLogging) {
    t0 = microtime();
  }

  uint8_t type = kCopyHeap;
  auto start_addr = reinterpret_cast<uint64_t>(heap_header->copy_start);
  auto len =
      (reinterpret_cast<uint64_t>(heap_header->slab.get_base()) - start_addr) +
      heap_header->slab.get_usage();
  auto per_thread_len = (len - 1) / kTransmitHeapNumThreads + 1;
  uint64_t req_start_addrs[kTransmitHeapNumThreads];
  uint64_t req_lens[kTransmitHeapNumThreads];

  for (uint32_t i = 0; i < kTransmitHeapNumThreads; i++) {
    req_start_addrs[i] = start_addr + i * per_thread_len;
    req_lens[i] = (i != kTransmitHeapNumThreads - 1)
                      ? per_thread_len
                      : start_addr + len - req_start_addrs[i];
    std::vector<iovec> task{
        {&type, sizeof(type)},
        {&req_start_addrs[i], sizeof(req_start_addrs[i])},
        {&req_lens[i], sizeof(req_lens[i])},
        {reinterpret_cast<std::byte *>(req_start_addrs[i]), req_lens[i]}};
    if (i < PressureHandler::kNumAuxHandlers) {
      // Dispatch to aux handler.
      Runtime::pressure_handler->dispatch_aux_tcp_task(i, std::move(task));
    } else {
      // Execute the task itself.
      BUG_ON(c->WritevFull(std::span<const iovec>(task), /* nt = */ true,
                           /* poll = */ true) < 0);
    }
  }

  Runtime::pressure_handler->wait_aux_tasks();

  if constexpr (kEnableLogging) {
    t1 = microtime();
    preempt_disable();
    std::cout << "Transmit heap: addr = " << heap_header << ", size = " << len
              << ", time_us = " << t1 - t0 << ", num heaps left = "
              << Runtime::heap_manager->get_num_present_heaps() << std::endl;
    preempt_enable();
  }
}

void Migrator::transmit_mutexes(rt::TcpConn *c, std::vector<Mutex *> mutexes) {
  size_t num_mutexes = mutexes.size();

  if (num_mutexes) {
    const iovec iovecs[] = {{&num_mutexes, sizeof(num_mutexes)},
                            {mutexes.data(), num_mutexes * sizeof(Mutex *)}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ false,
                         /* poll = */ true) < 0);
  } else {
    BUG_ON(c->WriteFull(&num_mutexes, sizeof(num_mutexes), /* nt = */ false,
                        /* poll = */ true) < 0);
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
    BUG_ON(!num_threads);
    BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                        /* poll = */ true) < 0);
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
    list_head_init(waiters);
  }
}

void Migrator::transmit_condvars(rt::TcpConn *c,
                                 std::vector<CondVar *> condvars) {
  size_t num_condvars = condvars.size();

  if (num_condvars) {
    const iovec iovecs[] = {
        {&num_condvars, sizeof(num_condvars)},
        {condvars.data(), num_condvars * sizeof(CondVar *)}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ false,
                         /* poll = */ true) < 0);
  } else {
    BUG_ON(c->WriteFull(&num_condvars, sizeof(num_condvars), /* nt = */ false,
                        /* poll = */ true) < 0);
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
    BUG_ON(!num_threads);
    BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                        /* poll = */ true) < 0);
    for (auto th : ths) {
      transmit_one_thread(c, th);
    }
    list_head_init(waiters);
  }
}

void Migrator::transmit_time(rt::TcpConn *c, Time *time) {
  uint64_t migrator_tsc = rdtscp(nullptr) - start_tsc;
  int64_t sum_tsc = migrator_tsc + time->offset_tsc_;

  const auto &timer_entries_list = time->entries_;
  size_t num_entries = timer_entries_list.size();
  const iovec iovecs[] = {{&sum_tsc, sizeof(sum_tsc)},
                          {&num_entries, sizeof(num_entries)}};
  BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ false, /* poll = */ true) <
         0);

  if (num_entries) {
    auto timer_entries_arr = std::make_unique<timer_entry *[]>(num_entries);
    std::copy(timer_entries_list.begin(), timer_entries_list.end(),
              timer_entries_arr.get());
    BUG_ON(c->WriteFull(timer_entries_arr.get(),
                        sizeof(timer_entry *) * num_entries,
                        /* nt = */ false, /* poll = */ true) < 0);

    for (size_t i = 0; i < num_entries; i++) {
      auto *entry = timer_entries_arr[i];
      timer_cancel(entry);
      auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
      transmit_one_thread(c, arg->th);
    }
  }
}

void Migrator::transmit_one_thread(rt::TcpConn *c, thread_t *thread) {
  size_t nu_state_size;
  auto *nu_state = thread_get_nu_state(thread, &nu_state_size);
  BUG_ON(c->WriteFull(nu_state, nu_state_size, /* nt = */ false,
                      /* poll = */ true) < 0);

  auto stack_range = get_obj_stack_range(thread);
  auto stack_len = stack_range.end - stack_range.start;
  BUG_ON(c->WriteFull(reinterpret_cast<void *>(stack_range.start), stack_len,
                      /* nt = */ false,
                      /* poll = */ true) < 0);
}

void Migrator::transmit_threads(rt::TcpConn *c,
                                const std::vector<thread_t *> &threads) {
  uint64_t num_threads = threads.size();
  BUG_ON(c->WriteFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                      /* poll = */ true) < 0);
  for (auto thread : threads) {
    transmit_one_thread(c, thread);
  }
}

void Migrator::transmit_stack_cluster_mmap_task(rt::TcpConn *c) {
  auto stack_cluster = Runtime::stack_manager->get_range();
  BUG_ON(c->WriteFull(&stack_cluster, sizeof(stack_cluster), /* nt = */ false,
                      /* poll = */ true) < 0);
}

void Migrator::transmit_heap_mmap_populate_ranges(
    rt::TcpConn *c, const std::vector<HeapRange> &heaps) {
  uint64_t size = heaps.size() * sizeof(HeapRange);
  if (size) {
    const iovec iovecs[] = {{&size, sizeof(size)},
                            {const_cast<HeapRange *>(heaps.data()), size}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ false,
                         /* poll = */ true) < 0);
  } else {
    BUG_ON(c->WriteFull(&size, sizeof(size), /* nt = */ false,
                        /* poll = */ true) < 0);
  }
}

void Migrator::transmit(rt::TcpConn *c, HeapHeader *heap_header,
                        struct list_head *paused_ths_list) {
  transmit_heap(c, heap_header);

  std::vector<thread_t *> ready_threads;
  std::vector<Mutex *> mutexes;
  std::vector<CondVar *> condvars;

  void *raw_th;
  list_for_each_off(paused_ths_list, raw_th, thread_link_offset) {
    auto *th = reinterpret_cast<thread_t *>(raw_th);
    ready_threads.push_back(th);
  }

  auto all_blocked_syncers = heap_header->blocked_syncer.get_all();
  for (auto [raw_ptr, type] : all_blocked_syncers) {
    switch (type) {
    case BlockedSyncer::kMutex:
      mutexes.push_back(reinterpret_cast<Mutex *>(raw_ptr));
      break;
    case BlockedSyncer::kCondVar:
      condvars.push_back(reinterpret_cast<CondVar *>(raw_ptr));
      break;
    default:
      BUG();
    }
  }

  transmit_mutexes(c, mutexes);
  transmit_condvars(c, condvars);
  transmit_time(c, &heap_header->time);
  transmit_threads(c, ready_threads);
}

bool Migrator::try_mark_heap_migrating(HeapHeader *heap_header) {
  if (unlikely(!Runtime::heap_manager->remove_for_migration(heap_header))) {
    return false;
  }
  heap_header->rcu_lock.writer_sync(/* poll = */ true);
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
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ false,
                         /* poll = */ true) < 0);
  }
}

void Migrator::handle_unmap(rt::TcpConn *c) {
  uint64_t num_heaps;
  VAddrRange stack_cluster;
  const iovec iovecs[] = {{&num_heaps, sizeof(num_heaps)},
                          {&stack_cluster, sizeof(stack_cluster)}};
  BUG_ON(c->ReadvFull(std::span(iovecs), /* nt = */ false,
                      /* poll = */ true) <= 0);
  HeapHeader *heaps[num_heaps];
  BUG_ON(c->ReadFull(heaps, sizeof(heaps), /* nt = */ false,
                     /* poll = */ true) <= 0);

  Runtime::stack_manager->add_ref_cnt(stack_cluster, -num_heaps);
  for (uint64_t i = 0; i < num_heaps; i++) {
    Runtime::heap_manager->deallocate(heaps[i]);
  }
}

void Migrator::init_aux_handlers(uint32_t dest_ip) {
  uint8_t type = kEnablePoll;
  std::vector<iovec> task{{&type, sizeof(type)}};

  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    auto aux_migration_conn = migrator_conn_mgr_.get(dest_ip);
    Runtime::pressure_handler->init_aux_handler(i,
                                                std::move(aux_migration_conn));
    Runtime::pressure_handler->dispatch_aux_tcp_task(i, std::move(task));
  }
  Runtime::pressure_handler->wait_aux_tasks();
}

void Migrator::finish_aux_handlers() {
  uint8_t type = kDisablePoll;
  std::vector<iovec> task{{&type, sizeof(type)}};

  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    Runtime::pressure_handler->dispatch_aux_tcp_task(i, std::move(task));
  }
  Runtime::pressure_handler->wait_aux_tasks();
}

void Migrator::callback() {
  bool dummy;
  for (auto *c : callback_conns_) {
    BUG_ON(c->WriteFull(&dummy, sizeof(dummy), /* nt = */ false,
                        /* poll = */ true) != sizeof(dummy));
  }
}

void Migrator::migrate(Resource resource, std::vector<HeapRange> heaps) {
  if (!ever_migrated_) {
    ever_migrated_ = true;
    callback();
  }

  const uint32_t num_total_heaps = heaps.size();
  uint32_t num_migrated_heaps = 0;
  std::vector<HeapRange> choosen_heaps;
  while (num_total_heaps - num_migrated_heaps > 0) {
    uint32_t num_choosen_heaps = std::min(kMaxNumHeapsPerMigration,
                                          num_total_heaps - num_migrated_heaps);
    auto ratio = static_cast<float>(num_choosen_heaps) / num_total_heaps;
    Resource choosen_resource;
    choosen_resource.cores =
        static_cast<uint32_t>(ratio * resource.cores + 0.5);
    choosen_resource.mem_mbs =
        static_cast<uint32_t>(ratio * resource.mem_mbs + 0.5);
    choosen_heaps.clear();
    for (uint32_t i = 0; i < num_choosen_heaps; i++) {
      choosen_heaps.push_back(heaps[num_migrated_heaps++]);
    }
    __migrate(choosen_resource, choosen_heaps);
  }
}

void Migrator::__migrate(Resource resource, std::vector<HeapRange> heaps) {
  auto dest_ip =
      Runtime::controller_client->get_migration_dest(resource);
  BUG_ON(!dest_ip);
  auto migration_conn = migrator_conn_mgr_.get(dest_ip);
  auto *conn = migration_conn.get_tcp_conn();
  init_aux_handlers(dest_ip);

  uint8_t type = kMigrate;
  BUG_ON(conn->WriteFull(&type, sizeof(type), /* nt = */ false,
                         /* poll = */ true) < 0);
  transmit_heap_mmap_populate_ranges(conn, heaps);
  transmit_stack_cluster_mmap_task(conn);

  std::vector<HeapHeader *> migrated_heaps;
  std::vector<HeapHeader *> destructed_heaps;
  migrated_heaps.reserve(heaps.size());
  for (auto [heap_header, _] : heaps) {
    Runtime::controller_client->update_location(to_obj_id(heap_header),
                                                dest_ip);
    if (unlikely(!try_mark_heap_migrating(heap_header))) {
      destructed_heaps.push_back(heap_header);
      continue;
    }
    migrated_heaps.push_back(heap_header);
    auto *paused_ths_list = pause_all_migrating_threads(heap_header);
    transmit(conn, heap_header, paused_ths_list);
    gc_migrated_threads();
    Runtime::pressure_handler->dispatch_aux_dealloc_task(0, heap_header);
  }

  unmap_destructed_heaps(conn, &destructed_heaps);
  finish_aux_handlers();
}

void Migrator::load_heap(rt::TcpConn *c, HeapHeader *heap_header) {
  [[maybe_unused]] uint64_t t0, t1;
  if constexpr (kEnableLogging) {
    t0 = microtime();
  }

  heap_header->pending_load_cnt += kTransmitHeapNumThreads;

  uint8_t type;
  BUG_ON(c->ReadFull(&type, sizeof(type), /* nt = */ false,
                     /* poll = */ true) <= 0);
  BUG_ON(type != kCopyHeap);
  handle_copy_heap(c);

  Runtime::heap_manager->setup(heap_header,
                               /* migratable = */ false,
                               /* from_migration = */ true);
  heap_header->cpu_load.reset();
  while (heap_header->pending_load_cnt.load()) {
    unblock_and_relax();
  }

  auto *slab = &heap_header->slab;
  nu::SlabAllocator::register_slab_by_id(slab, slab->get_id());

  if constexpr (kEnableLogging) {
    t1 = microtime();
    preempt_disable();
    std::cout << "Load heap: addr = " << heap_header
              << ", time_us = " << t1 - t0 << std::endl;
    preempt_enable();
  }
}

thread_t *Migrator::load_one_thread(rt::TcpConn *c, HeapHeader *heap_header) {
  size_t nu_state_size;
  thread_get_nu_state(thread_self(), &nu_state_size);
  auto nu_state = std::make_unique<uint8_t[]>(nu_state_size);
  BUG_ON(c->ReadFull(nu_state.get(), nu_state_size, /* nt = */ false,
                     /* poll = */ true) <= 0);
  auto *th = create_migrated_thread(nu_state.get());

  auto stack_range = get_obj_stack_range(th);
  auto stack_len = stack_range.end - stack_range.start;
  BUG_ON(c->ReadFull(reinterpret_cast<void *>(stack_range.start), stack_len,
                     /* nt = */ false,
                     /* poll = */ true) <= 0);

  auto *nu_thread = reinterpret_cast<Thread *>(thread_get_nu_thread(th));
  if (is_in_heap(nu_thread, heap_header) ||
      is_in_stack(nu_thread, stack_range)) {
    BUG_ON(!nu_thread->th_);
    nu_thread->th_ = th;
  }
  return th;
}

void Migrator::load_mutexes(rt::TcpConn *c, HeapHeader *heap_header) {
  size_t num_mutexes;
  BUG_ON(c->ReadFull(&num_mutexes, sizeof(num_mutexes), /* nt = */ false,
                     /* poll = */ true) <= 0);

  if (num_mutexes) {
    auto mutexes = std::make_unique<Mutex *[]>(num_mutexes);
    BUG_ON(c->ReadFull(mutexes.get(), num_mutexes * sizeof(Mutex *),
                       /* nt = */ false,
                       /* poll = */ true) <= 0);

    for (size_t i = 0; i < num_mutexes; i++) {
      auto *mutex = mutexes[i];
      heap_header->blocked_syncer.add(mutex, BlockedSyncer::kMutex);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                         /* poll = */ true) <= 0);
      // FIXME
      // heap_header->migrated_wg.Add(num_threads);

      auto *waiters = mutex->get_waiters();
      if (heap_header->will_be_copied_on_migration(mutex)) {
        list_head_init(waiters);
      }
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
  BUG_ON(c->ReadFull(&num_condvars, sizeof(num_condvars), /* nt = */ false,
                     /* poll = */ true) <= 0);

  if (num_condvars) {
    auto condvars = std::make_unique<CondVar *[]>(num_condvars);
    BUG_ON(c->ReadFull(condvars.get(), num_condvars * sizeof(CondVar *),
                       /* nt = */ false,
                       /* poll = */ true) <= 0);

    for (size_t i = 0; i < num_condvars; i++) {
      auto *condvar = condvars[i];
      heap_header->blocked_syncer.add(condvar, BlockedSyncer::kCondVar);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                         /* poll = */ true) <= 0);
      // FIXME
      // heap_header->migrated_wg.Add(num_threads);

      auto *waiters = condvar->get_waiters();
      if (heap_header->will_be_copied_on_migration(condvar)) {
        list_head_init(waiters);
      }
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
  auto &time = heap_header->time;

  int64_t sum_tsc;
  size_t num_entries;
  const iovec iovecs[] = {{&sum_tsc, sizeof(sum_tsc)},
                          {&num_entries, sizeof(num_entries)}};
  BUG_ON(c->ReadvFull(std::span(iovecs), /* nt = */ false,
                      /* poll = */ true) <= 0);
  // FIXME
  // heap_header->migrated_wg.Add(num_entries);

  auto loader_tsc = rdtscp(nullptr) - start_tsc;
  time.offset_tsc_ = sum_tsc - loader_tsc;

  if (num_entries) {
    auto timer_entries = std::make_unique<timer_entry *[]>(num_entries);
    BUG_ON(c->ReadFull(timer_entries.get(), sizeof(timer_entry *) * num_entries,
                       /* nt = */ false,
                       /* poll = */ true) <= 0);

    for (size_t i = 0; i < num_entries; i++) {
      auto *entry = timer_entries[i];
      auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
      auto *th = load_one_thread(c, heap_header);
      arg->th = th;
      {
        rt::SpinGuard guard(&time.spin_);
        time.entries_.push_back(entry);
        arg->iter = --time.entries_.end();
      }
      entry->armed = false;
      timer_start(entry, time.to_physical_us(arg->logical_deadline_us));
    }
  }
}

void Migrator::load_threads(rt::TcpConn *c, HeapHeader *heap_header) {
  uint64_t num_threads;
  BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                     /* poll = */ true) <= 0);
  // FIXME
  // heap_header->migrated_wg.Add(num_threads);

  for (uint64_t i = 0; i < num_threads; i++) {
    auto *th = load_one_thread(c, heap_header);
    thread_ready(th);
  }
}

VAddrRange Migrator::load_stack_cluster_mmap_task(rt::TcpConn *c) {
  VAddrRange stack_cluster;
  BUG_ON(c->ReadFull(&stack_cluster, sizeof(stack_cluster), /* nt = */ false,
                     /* poll = */ true) <= 0);
  return stack_cluster;
}

std::vector<HeapRange>
Migrator::load_heap_mmap_populate_ranges(rt::TcpConn *c) {
  std::vector<HeapRange> populate_ranges;
  uint64_t size;

  BUG_ON(c->ReadFull(&size, sizeof(size), /* nt = */ false,
                     /* poll = */ true) <= 0);

  if (size) {
    populate_ranges.resize(size / sizeof(HeapRange));
    BUG_ON(c->ReadFull(&populate_ranges[0], size, /* nt = */ false,
                       /* poll = */ true) <= 0);
  }

  return populate_ranges;
}

void Migrator::load(rt::TcpConn *c) {
  auto populate_ranges = load_heap_mmap_populate_ranges(c);
  for (auto &range : populate_ranges) {
    rt::access_once(range.heap_header->status) = kLoading;
  }
  rt::Thread([&] {
    for (auto &range : populate_ranges) {
      Runtime::heap_manager->mmap_populate(range.heap_header, range.len);
    }
  }).Detach();

  auto stack_cluster = load_stack_cluster_mmap_task(c);
  Runtime::stack_manager->add_ref_cnt(stack_cluster, populate_ranges.size());

  for (auto &range : populate_ranges) {
    auto *heap_header = range.heap_header;

    load_heap(c, heap_header);

    load_mutexes(c, heap_header);
    load_condvars(c, heap_header);
    load_time(c, heap_header);

    Runtime::heap_manager->insert(heap_header);

    load_threads(c, heap_header);

    // Wakeup the blocked threads.
    heap_header->spin_lock.lock();
    heap_header->cond_var.signal_all();
    heap_header->spin_lock.unlock();

    // FIXME
    // rt::Thread([heap_header, stack_cluster] {
    //   heap_header->migrated_wg.Wait();
    //   rt::access_once(heap_header->migratable) = true;
    //   Runtime::stack_manager->add_ref_cnt(stack_cluster, -1);
    // }).Detach();
  }
}

void Migrator::reserve_conns(uint32_t dest_server_ip) {
  std::vector<MigratorConn> migrator_conns;
  migrator_conns.reserve(kDefaultNumReservedConns);
  for (uint32_t i = 0; i < kDefaultNumReservedConns; i++) {
    migrator_conns.emplace_back(migrator_conn_mgr_.get(dest_server_ip));
  }
}

void Migrator::forward_to_original_server(RPCReturnCode rc,
                                          RPCReturner *returner,
                                          uint64_t payload_len,
                                          const void *payload) {
  RuntimeSlabGuard guard;

  auto req_buf_len = sizeof(RPCReqForward) + payload_len;
  auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
  auto *req = reinterpret_cast<RPCReqForward *>(req_buf.get());
  std::construct_at(req);
  req->rc = rc;
  req->returner = *returner;
  req->stack_top = get_obj_stack_range(thread_self()).end;
  req->payload_len = payload_len;
  memcpy(req->payload, payload, payload_len);
  auto req_span = std::span(req_buf.get(), req_buf_len);
  RPCReturnBuffer return_buf;
  auto *client = Runtime::rpc_client_mgr->get_by_ip(thread_get_creator_ip());
  BUG_ON(client->Call(req_span, &return_buf) != kOk);
}

void Migrator::forward_to_client(RPCReqForward &req) {
  if (req.payload_len) {
    auto payload_buf =
        std::make_unique_for_overwrite<std::byte[]>(req.payload_len);
    memcpy(payload_buf.get(), req.payload, req.payload_len);
    auto span = std::span(payload_buf.get(), req.payload_len);
    req.returner.Return(req.rc, span,
                        [payload_buf = std::move(payload_buf)] {});
  } else {
    req.returner.Return(req.rc);
  }
  Runtime::stack_manager->put(reinterpret_cast<uint8_t *>(req.stack_top));
}

} // namespace nu

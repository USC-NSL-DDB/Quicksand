#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>

extern "C" {
#include <base/assert.h>
#include <net/ip.h>
#include <runtime/membarrier.h>
#include <runtime/timer.h>
}
#include <thread.h>
#include <runtime.h>

#include "nu/commons.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/migrator.hpp"
#include "nu/pressure_handler.hpp"
#include "nu/proclet_mgr.hpp"
#include "nu/proclet_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"
#include "nu/utils/cond_var.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/thread.hpp"

namespace nu {

constexpr static bool kEnableLogging = false;
constexpr static auto kMigrationDSCP = IPTOS_DSCP_CS0;

MigratorConn::MigratorConn() : tcp_conn_(nullptr), ip_(0), manager_(nullptr) {}

MigratorConn::MigratorConn(rt::TcpConn *tcp_conn, uint32_t ip,
                           MigratorConnManager *manager)
    : tcp_conn_(tcp_conn), ip_(ip), manager_(manager) {}

MigratorConn::~MigratorConn() { release(); }

void MigratorConn::release() {
  if (tcp_conn_) {
    manager_->put(ip_, tcp_conn_);
    tcp_conn_ = nullptr;
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

Migrator::Migrator() { callback_triggered_ = true; }

Migrator::~Migrator() { BUG(); }

void Migrator::handle_copy_proclet(rt::TcpConn *c) {
  ProcletHeader *proclet_header;
  uint64_t start_addr, len;
  const iovec iovecs[] = {{&proclet_header, sizeof(proclet_header)},
                          {&start_addr, sizeof(start_addr)},
                          {&len, sizeof(len)}};
  BUG_ON(c->ReadvFull(std::span(iovecs), /* nt = */ false, /* poll = */ true) <=
         0);
  BUG_ON(c->ReadFull(reinterpret_cast<uint8_t *>(start_addr), len,
                     /* nt = */ true, /* poll = */ true) <= 0);
  proclet_header->pending_load_cnt--;
}

inline void Migrator::handle_load(rt::TcpConn *c) {
  rt::Preempt p;
  rt::PreemptGuard g(&p);

  load(c);
}

void Migrator::handle_register_callback(rt::TcpConn *c) {
  callback_conns_.insert(c);
  callback_triggered_ = false;
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
            case kCopyProclet:
              handle_copy_proclet(c);
              break;
            case kMigrate:
              handle_load(c);
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

inline void throttle(uint64_t len, uint64_t real_us) {
  uint64_t expected_us = len / Migrator::kMigrationThrottleGBs / 1000;
  if (expected_us > real_us) {
    delay_us(expected_us - real_us);
  }
}

void Migrator::transmit_proclet(rt::TcpConn *c, ProcletHeader *proclet_header) {
  constexpr bool kMonitorTime =
      (kEnableLogging || kMigrationThrottleGBs > 0 || kMigrationDelayUs);
  [[maybe_unused]] uint64_t t0, t1;

  if constexpr (kMonitorTime) {
    t0 = microtime();
  }

  uint8_t type = kCopyProclet;
  auto start_addr = reinterpret_cast<uint64_t>(proclet_header->copy_start);
  auto len = (reinterpret_cast<uint64_t>(proclet_header->slab.get_base()) -
              start_addr) +
             proclet_header->slab.get_usage();
  auto per_thread_len = (len - 1) / kTransmitProcletNumThreads + 1;
  uint64_t req_start_addrs[kTransmitProcletNumThreads];
  uint64_t req_lens[kTransmitProcletNumThreads];

  for (uint32_t i = 0; i < kTransmitProcletNumThreads; i++) {
    req_start_addrs[i] = start_addr + i * per_thread_len;
    req_lens[i] = (i != kTransmitProcletNumThreads - 1)
                      ? per_thread_len
                      : start_addr + len - req_start_addrs[i];
    std::vector<iovec> task{
        {&type, sizeof(type)},
        {&proclet_header, sizeof(proclet_header)},
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

  if constexpr (kMonitorTime) {
    t1 = microtime();
  }

  if constexpr (kMigrationThrottleGBs > 0) {
    throttle(len, t1 - t0);
    t1 = microtime();
  }

  if constexpr (kMigrationDelayUs) {
    auto remote_ip = c->RemoteAddr().ip;
    auto delayed = delayed_srv_ips_.contains(remote_ip);
    if (!delayed) {
      delayed_srv_ips_.insert(remote_ip);
      delay_us(kMigrationDelayUs);
      t1 = microtime();
    }
  }

  if constexpr (kEnableLogging) {
    preempt_disable();
    std::cout << "Transmit proclet: addr = " << proclet_header
              << ", size = " << len << ", time_us = " << t1 - t0
              << ", num proclets left = "
              << Runtime::proclet_manager->get_num_present_proclets()
              << std::endl;
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

  auto stack_range = Runtime::get_proclet_stack_range(thread);
  auto stack_len = stack_range.end - stack_range.start;
  BUG_ON(c->WriteFull(reinterpret_cast<void *>(stack_range.start), stack_len,
                      /* nt = */ false,
                      /* poll = */ true) < 0);
  Runtime::stack_manager->free(reinterpret_cast<uint8_t *>(stack_range.end));
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

void Migrator::transmit_proclet_migration_tasks(
    rt::TcpConn *c, std::vector<ProcletMigrationTask>::const_iterator begin,
    std::vector<ProcletMigrationTask>::const_iterator end) {
  uint64_t size = (end - begin) * sizeof(decltype(*begin));
  if (size) {
    auto *begin_raw_ptr = reinterpret_cast<const void *>(&*begin);
    const iovec iovecs[] = {
        {&size, sizeof(size)},
        {const_cast<void *>(begin_raw_ptr), size}};
    BUG_ON(c->WritevFull(std::span(iovecs), /* nt = */ false,
                         /* poll = */ true) < 0);
  } else {
    BUG_ON(c->WriteFull(&size, sizeof(size), /* nt = */ false,
                        /* poll = */ true) < 0);
  }
}

void Migrator::update_proclet_location(rt::TcpConn *c,
                                       ProcletHeader *proclet_header) {
  Runtime::controller_client->update_location(to_proclet_id(proclet_header),
                                              c->RemoteAddr().ip);
  // Wakeup the requests blocked after marking the proclet as absent so that
  // they will be immediately rejected.
  proclet_header->spin_lock.lock();
  proclet_header->status() = kAbsent;
  proclet_header->cond_var.signal_all();
  proclet_header->spin_lock.unlock();
}

void Migrator::transmit(rt::TcpConn *c, ProcletHeader *proclet_header,
                        struct list_head *paused_ths_list) {
  transmit_proclet(c, proclet_header);

  std::vector<thread_t *> ready_threads;
  std::vector<Mutex *> mutexes;
  std::vector<CondVar *> condvars;

  void *raw_th;
  list_for_each_off(paused_ths_list, raw_th, thread_link_offset) {
    auto *th = reinterpret_cast<thread_t *>(raw_th);
    ready_threads.push_back(th);
  }

  auto all_blocked_syncers = proclet_header->blocked_syncer.get_all();
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
  transmit_time(c, &proclet_header->time);
  transmit_threads(c, ready_threads);

  update_proclet_location(c, proclet_header);
}

bool Migrator::try_mark_proclet_migrating(ProcletHeader *proclet_header) {
  if (unlikely(
          !Runtime::proclet_manager->remove_for_migration(proclet_header))) {
    return false;
  }
  proclet_header->rcu_lock.writer_sync(/* poll = */ true);
  return true;
}

void Migrator::aux_handlers_enable_polling(uint32_t dest_ip) {
  uint8_t type = kEnablePoll;

  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    auto aux_migration_conn = migrator_conn_mgr_.get(dest_ip);
    Runtime::pressure_handler->update_aux_handler_state(
        i, std::move(aux_migration_conn));
    std::vector<iovec> task{{&type, sizeof(type)}};
    Runtime::pressure_handler->dispatch_aux_tcp_task(i, std::move(task));
  }
  Runtime::pressure_handler->wait_aux_tasks();
}

void Migrator::aux_handlers_disable_polling() {
  uint8_t type = kDisablePoll;

  for (uint32_t i = 0; i < PressureHandler::kNumAuxHandlers; i++) {
    std::vector<iovec> task{{&type, sizeof(type)}};
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

uint32_t Migrator::migrate(Resource resource,
                           const std::vector<ProcletMigrationTask> &tasks) {
  if (!callback_triggered_) {
    callback_triggered_ = true;
    callback();
  }

  const uint32_t max_num_proclets_per_migration =
      get_max_num_proclets_per_migration();
  const uint32_t num_total_proclets = tasks.size();
  uint32_t num_migrated_proclets = 0;
  std::vector<ProcletMigrationTask> choosen_tasks;

  while (num_total_proclets - num_migrated_proclets > 0) {
    uint32_t num_choosen_proclets =
        std::min(max_num_proclets_per_migration,
                 num_total_proclets - num_migrated_proclets);
    auto ratio = static_cast<float>(num_choosen_proclets) / num_total_proclets;
    Resource choosen_resource;
    choosen_resource.cores =
        static_cast<uint32_t>(ratio * resource.cores + 0.5);
    choosen_resource.mem_mbs =
        static_cast<uint32_t>(ratio * resource.mem_mbs + 0.5);
    choosen_tasks.clear();
    for (uint32_t i = 0; i < num_choosen_proclets; i++) {
      choosen_tasks.push_back(tasks[num_migrated_proclets + i]);
    }
    auto delta = __migrate(choosen_resource, choosen_tasks);
    num_migrated_proclets += delta;
    if (delta < choosen_tasks.size()) {
      break;
    }
  }

  return num_migrated_proclets;
}

void Migrator::pause_migrating_threads(ProcletHeader *proclet_header) {
  Runtime::pressure_handler->dispatch_aux_pause_task(0);
  pause_migrating_ths_main(proclet_header);
}

void Migrator::post_migration_cleanup(ProcletHeader *proclet_header) {
  rt::Thread([proclet_header] {
    Runtime::proclet_manager->cleanup(proclet_header,
                                      /* for_migration = */ true);
  }).Detach();
}

void skip_proclet(rt::TcpConn *conn, ProcletHeader *proclet_header) {
  uint8_t type = kSkipProclet;
  BUG_ON(conn->WriteFull(&type, sizeof(type), /* nt = */ false,
                         /* poll = */ true) < 0);
}

bool receive_approval(rt::TcpConn *c) {
  bool approval;
  BUG_ON(c->ReadFull(&approval, sizeof(approval),
                     /* nt = */ false, /* poll = */ true) <= 0);
  return approval;
}

void send_done(rt::TcpConn *c) {
  bool dummy = false;
  BUG_ON(c->WriteFull(&dummy, sizeof(dummy), /* nt = */ false,
                      /* poll = */ true) < 0);
}

uint32_t Migrator::__migrate(Resource resource,
                             const std::vector<ProcletMigrationTask> &tasks) {
  auto it = tasks.begin();
  bool loader_approval = true;

  do {
    auto migration_dest =
        Runtime::controller_client->acquire_migration_dest(resource);
    if (unlikely(!migration_dest)) {
      break;
    }

    auto migration_conn = migrator_conn_mgr_.get(migration_dest.get_ip());
    auto *conn = migration_conn.get_tcp_conn();
    aux_handlers_enable_polling(migration_dest.get_ip());

    uint8_t type = kMigrate;
    BUG_ON(conn->WriteFull(&type, sizeof(type), /* nt = */ false,
                           /* poll = */ true) < 0);
    transmit_proclet_migration_tasks(conn, it, tasks.end());

    while (it != tasks.end()) {
      loader_approval = receive_approval(conn);
      if (unlikely(!loader_approval ||
                   !Runtime::pressure_handler->has_pressure())) {
        break;
      }

      auto *proclet_header = it++->header;
      if (unlikely(!try_mark_proclet_migrating(proclet_header))) {
        skip_proclet(conn, proclet_header);
        continue;
      }

      pause_migrating_threads(proclet_header);
      transmit(conn, proclet_header, &all_migrating_ths);
      gc_migrated_threads();
      post_migration_cleanup(proclet_header);
    }

    send_done(conn);

    for (auto tmp_it = it; tmp_it != tasks.end(); tmp_it++) {
      auto *proclet_header = tmp_it->header;
      receive_approval(conn);
      skip_proclet(conn, proclet_header);
    }

    aux_handlers_disable_polling();

  } while (!loader_approval);

  return it - tasks.begin();
}

bool Migrator::load_proclet(rt::TcpConn *c, ProcletHeader *proclet_header,
                            uint64_t capacity) {
  constexpr bool kMonitorTime =
      (kEnableLogging || kMigrationThrottleGBs > 0 || kMigrationDelayUs);
  [[maybe_unused]] uint64_t t0, t1;

  if constexpr (kMonitorTime) {
    t0 = microtime();
  }

  proclet_header->pending_load_cnt += kTransmitProcletNumThreads;

  uint8_t type;
  BUG_ON(c->ReadFull(&type, sizeof(type), /* nt = */ false,
                     /* poll = */ true) <= 0);
  if (unlikely(type == kSkipProclet)) {
    return false;
  }
  BUG_ON(type != kCopyProclet);
  handle_copy_proclet(c);

  Runtime::proclet_manager->setup(proclet_header, capacity,
                                  /* migratable = */ false,
                                  /* from_migration = */ true);
  while (proclet_header->pending_load_cnt.load()) {
    unblock_and_relax();
  }

  auto *slab = &proclet_header->slab;
  nu::SlabAllocator::register_slab_by_id(slab, slab->get_id());

  if constexpr (kMonitorTime) {
    t1 = microtime();
  }

  if constexpr (kMigrationThrottleGBs > 0) {
    throttle(proclet_header->slab.get_usage(), t1 - t0);
    t1 = microtime();
  }

  if constexpr (kMigrationDelayUs) {
    auto remote_ip = c->RemoteAddr().ip;
    auto delayed = delayed_srv_ips_.contains(remote_ip);
    if (!delayed) {
      delayed_srv_ips_.insert(remote_ip);
      delay_us(kMigrationDelayUs);
      t1 = microtime();
    }
  }

  if constexpr (kEnableLogging) {
    preempt_disable();
    std::cout << "Load proclet: addr = " << proclet_header
              << ", time_us = " << t1 - t0 << std::endl;
    preempt_enable();
  }
  return true;
}

thread_t *Migrator::load_one_thread(rt::TcpConn *c,
                                    ProcletHeader *proclet_header) {
  proclet_header->thread_cnt.inc_unsafe();

  size_t nu_state_size;
  thread_get_nu_state(thread_self(), &nu_state_size);
  auto nu_state = std::make_unique<uint8_t[]>(nu_state_size);
  BUG_ON(c->ReadFull(nu_state.get(), nu_state_size, /* nt = */ false,
                     /* poll = */ true) <= 0);
  auto *th = create_migrated_thread(nu_state.get());

  auto stack_range = Runtime::get_proclet_stack_range(th);
  auto stack_len = stack_range.end - stack_range.start;
  BUG_ON(c->ReadFull(reinterpret_cast<void *>(stack_range.start), stack_len,
                     /* nt = */ false,
                     /* poll = */ true) <= 0);
  return th;
}

void Migrator::load_mutexes(rt::TcpConn *c, ProcletHeader *proclet_header) {
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
      proclet_header->blocked_syncer.add(mutex, BlockedSyncer::kMutex);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                         /* poll = */ true) <= 0);

      auto *waiters = mutex->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto *th = load_one_thread(c, proclet_header);
        auto *th_link = reinterpret_cast<list_node *>(
            reinterpret_cast<uintptr_t>(th) + thread_link_offset);
        list_add_tail(waiters, th_link);
      }
    }
  }
}

void Migrator::load_condvars(rt::TcpConn *c, ProcletHeader *proclet_header) {
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
      proclet_header->blocked_syncer.add(condvar, BlockedSyncer::kCondVar);

      size_t num_threads;
      BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                         /* poll = */ true) <= 0);

      auto *waiters = condvar->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto *th = load_one_thread(c, proclet_header);
        auto *th_link = reinterpret_cast<list_node *>(
            reinterpret_cast<uintptr_t>(th) + thread_link_offset);
        list_add_tail(waiters, th_link);
      }
    }
  }
}

void Migrator::load_time(rt::TcpConn *c, ProcletHeader *proclet_header) {
  auto &time = proclet_header->time;

  int64_t sum_tsc;
  size_t num_entries;
  const iovec iovecs[] = {{&sum_tsc, sizeof(sum_tsc)},
                          {&num_entries, sizeof(num_entries)}};
  BUG_ON(c->ReadvFull(std::span(iovecs), /* nt = */ false,
                      /* poll = */ true) <= 0);

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
      auto *th = load_one_thread(c, proclet_header);
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

void Migrator::load_threads(rt::TcpConn *c, ProcletHeader *proclet_header) {
  uint64_t num_threads;
  BUG_ON(c->ReadFull(&num_threads, sizeof(num_threads), /* nt = */ false,
                     /* poll = */ true) <= 0);

  for (uint64_t i = 0; i < num_threads; i++) {
    auto *th = load_one_thread(c, proclet_header);
    thread_ready(th);
  }
}

std::vector<ProcletMigrationTask> Migrator::load_proclet_migration_tasks(
    rt::TcpConn *c) {
  std::vector<ProcletMigrationTask> tasks;
  uint64_t size;

  BUG_ON(c->ReadFull(&size, sizeof(size), /* nt = */ false,
                     /* poll = */ true) <= 0);

  if (size) {
    tasks.resize(size / sizeof(ProcletMigrationTask));
    BUG_ON(c->ReadFull(&tasks[0], size, /* nt = */ false,
                       /* poll = */ true) <= 0);
  }

  return tasks;
}

void issue_approval(rt::TcpConn *c, bool approval) {
  BUG_ON(c->WriteFull(&approval, sizeof(approval), /* nt = */ false,
                      /* poll = */ true) < 0);
}

void receive_done(rt::TcpConn *c) {
  bool dummy;
  BUG_ON(c->ReadFull(&dummy, sizeof(dummy),
                     /* nt = */ false, /* poll = */ true) <= 0);
}

void Migrator::load(rt::TcpConn *c) {
  auto tasks = load_proclet_migration_tasks(c);
  rt::Thread mmap_th([tasks] {
    for (auto &[header, _, size] : tasks) {
      Runtime::proclet_manager->madvise_populate(header, size);
    }
  });

  std::vector<ProcletHeader *> loaded_proclets;
  std::vector<std::pair<ProcletHeader *, uint64_t>> skipped_proclets;

  for (auto &[proclet_header, capacity, size] : tasks) {
    bool approval = !Runtime::pressure_handler->has_real_pressure();
    issue_approval(c, approval);

    if (unlikely(!load_proclet(c, proclet_header, capacity))) {
      skipped_proclets.emplace_back(proclet_header, size);
      continue;
    }

    load_mutexes(c, proclet_header);
    load_condvars(c, proclet_header);
    load_time(c, proclet_header);
    Runtime::proclet_manager->insert(proclet_header);
    load_threads(c, proclet_header);
    loaded_proclets.emplace_back(proclet_header);
    // Wakeup the blocked threads.
    proclet_header->cond_var.signal_all();
  }

  receive_done(c);
  for (auto *proclet_header : loaded_proclets) {
    proclet_header->migratable = true;
  }

  if (unlikely(!skipped_proclets.empty())) {
    preempt_enable();
    mmap_th.Join();
    preempt_disable();
    for (auto [proclet, size] : skipped_proclets) {
      Runtime::proclet_manager->depopulate(proclet, size,
                                           /* defer = */ false);
    }
  } else {
    mmap_th.Detach();
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
  req->stack_top = Runtime::get_proclet_stack_range(thread_self()).end;
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

uint32_t Migrator::get_max_num_proclets_per_migration() const {
  return Migrator::kMaxPctProcletPerMigration *
             Runtime::proclet_manager->get_num_present_proclets() +
         1;
}

__attribute__((noinline)) void Migrator::transmit_thread_and_ret_val(
    std::unique_ptr<std::byte[]> *req_buf_ptr, uint64_t req_buf_len,
    ProcletID dest_id, uint8_t *proclet_stack) {
  auto req_buf = std::move(*req_buf_ptr);
  // Runtime::stack_manager->free(proclet_stack);

  auto req_span = std::span(req_buf.get(), req_buf_len);
  RPCReturnBuffer unused_buf;

retry:
  auto *rpc_client = Runtime::rpc_client_mgr->get_by_proclet_id(dest_id);
  auto rc = rpc_client->Call(req_span, &unused_buf);

  if (unlikely(rc == kErrWrongClient)) {
    Runtime::rpc_client_mgr->invalidate_cache(dest_id, rpc_client);
    goto retry;
  }

  rt::Exit();
}

}  // namespace nu

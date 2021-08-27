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
#include "nu/cond_var.hpp"
#include "nu/ctrl_client.hpp"
#include "nu/heap_mgr.hpp"
#include "nu/migrator.hpp"
#include "nu/mutex.hpp"
#include "nu/obj_server.hpp"
#include "nu/runtime.hpp"
#include "nu/runtime_alloc.hpp"

constexpr static bool kEnableLogging = false;

namespace nu {

Migrator::Migrator() {}

Migrator::~Migrator() {}

void Migrator::handle_forward(RPCReqForward &req) {
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

void Migrator::handle_migrate(const RPCReqMigrate &req) {
  auto *client = Runtime::rpc_client_mgr->get_by_ip(req.src_ip);

  std::vector<thread_t *> blocked_threads;
  auto *heap = req.heap;
  load_heap(client, req.map_task_desc);

  load_mutexes(client, heap, &blocked_threads);

  load_condvars(client, heap, &blocked_threads);

  load_time(client, heap, &blocked_threads);

  Runtime::heap_manager->insert(heap);

  rt::Thread([heap] {
    Runtime::controller_client->update_location(
        to_obj_id(heap), Runtime::obj_server->get_addr());
  }).Detach();

  load_unblocked_threads(client, heap, blocked_threads);

  rt::Thread([heap, stack_cluster = req.stack_cluster] {
    heap->forward_wg.Wait();
    rt::access_once(heap->migratable) = true;
    Runtime::stack_manager->add_ref_cnt(stack_cluster, -1);
  }).Detach();
}

void Migrator::handle_load_mutexes_info(const RPCReqLoadMutexesInfo &req,
                                        RPCReturner *returner) {
  std::vector<MutexInfo> mutex_infos;
  auto mutexes = req.heap_header->mutexes->all_keys();
  mutex_infos.reserve(mutexes.size());

  for (auto *mutex : mutexes) {
    mutex_infos.emplace_back(mutex, !list_empty(mutex->get_waiters()));
  }

  auto span = std::span(reinterpret_cast<std::byte *>(mutex_infos.data()),
                        sizeof(MutexInfo) * mutex_infos.size());
  returner->Return(kOk, span, [mutex_infos = std::move(mutex_infos)] {});
}

void Migrator::handle_load_mutex_thread_info(
    const RPCReqLoadMutexThreadInfo &req, RPCReturner *returner) {
  std::vector<ThreadInfo> thread_infos;
  auto *waiters = req.mutex->get_waiters();
  void *th_raw;
  list_for_each_off(waiters, th_raw, thread_link_offset) {
    auto *th = reinterpret_cast<thread_t *>(th_raw);
    thread_infos.push_back(acquire_thread_info(th));
  }

  auto span = std::span(reinterpret_cast<std::byte *>(thread_infos.data()),
                        sizeof(ThreadInfo) * thread_infos.size());
  returner->Return(kOk, span, [thread_infos = std::move(thread_infos)] {});
}

void Migrator::handle_load_condvars_info(const RPCReqLoadCondvarsInfo &req,
                                         RPCReturner *returner) {
  std::vector<CondvarInfo> condvar_infos;
  auto condvars = req.heap_header->condvars->all_keys();
  condvar_infos.reserve(condvars.size());

  for (auto *condvar : condvars) {
    condvar_infos.emplace_back(condvar, !list_empty(condvar->get_waiters()));
  }

  auto span = std::span(reinterpret_cast<std::byte *>(condvar_infos.data()),
                        sizeof(CondvarInfo) * condvar_infos.size());
  returner->Return(kOk, span, [condvar_infos = std::move(condvar_infos)] {});
}

void Migrator::handle_load_condvar_thread_info(
    const RPCReqLoadCondvarThreadInfo &req, RPCReturner *returner) {
  std::vector<ThreadInfo> thread_infos;
  auto *waiters = req.condvar->get_waiters();
  void *th_raw;
  list_for_each_off(waiters, th_raw, thread_link_offset) {
    auto *th = reinterpret_cast<thread_t *>(th_raw);
    thread_infos.push_back(acquire_thread_info(th));
  }

  auto span = std::span(reinterpret_cast<std::byte *>(thread_infos.data()),
                        sizeof(ThreadInfo) * thread_infos.size());
  returner->Return(kOk, span, [thread_infos = std::move(thread_infos)] {});
}

void Migrator::handle_load_time_info(const RPCReqLoadTimeInfo &req,
                                     RPCReturner *returner) {
  uint64_t migrator_tsc = rdtscp(nullptr) - start_tsc;
  int64_t sum_tsc = migrator_tsc + req.heap_header->time->offset_tsc_;

  std::vector<TimerEntryInfo> timer_entry_infos;
  const auto &timer_entries_list = req.heap_header->time->entries_;
  size_t num_entries = timer_entries_list.size();

  auto resp_buf_len =
      sizeof(RPCRespLoadTimeInfo) + sizeof(TimerEntryInfo) * num_entries;
  auto resp_buf = std::make_unique_for_overwrite<std::byte[]>(resp_buf_len);
  auto *resp = reinterpret_cast<RPCRespLoadTimeInfo *>(resp_buf.get());

  resp->sum_tsc = sum_tsc;
  resp->num_entries = num_entries;
  auto timer_entry_iter = timer_entries_list.begin();
  for (uint32_t i = 0; i < num_entries; i++, timer_entry_iter++) {
    auto *entry = *timer_entry_iter;
    timer_cancel(entry);
    auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
    resp->timer_entry_infos[i].entry = entry;
    resp->timer_entry_infos[i].thread_info = acquire_thread_info(arg->th);
  }

  auto span = std::span(resp_buf.get(), resp_buf_len);
  returner->Return(kOk, span, [resp_buf = std::move(resp_buf)] {});
}

void Migrator::handle_load_unblocked_threads(
    const RPCReqLoadUnblockedThreads &req, RPCReturner *returner) {
  std::unordered_set<thread_t *> blocked_threads;
  for (uint32_t i = 0; i < req.num_blocked_threads; i++) {
    blocked_threads.insert(req.threads[i]);
  }

  auto all_threads = req.heap_header->threads->all_keys();
  std::vector<thread_t *> running_threads;
  for (auto thread : all_threads) {
    if (!blocked_threads.contains(thread)) {
      running_threads.push_back(thread);
    }
  }

  if (!running_threads.empty()) {
    auto resp_buf_len = sizeof(ThreadInfo) * running_threads.size();
    auto resp_buf = std::make_unique_for_overwrite<std::byte[]>(resp_buf_len);
    auto *resp =
        reinterpret_cast<RPCRespLoadUnblockedThreads *>(resp_buf.get());
    for (uint32_t i = 0; i < running_threads.size(); i++) {
      resp->thread_infos[i] = acquire_thread_info(running_threads[i]);
    }

    auto span = std::span(resp_buf.get(), resp_buf_len);
    returner->Return(kOk, span, [resp_buf = std::move(resp_buf)] {});
  } else {
    returner->Return(kOk);
  }
}

std::unique_ptr<RPCRespMap> Migrator::handle_map(const RPCReqMap &req) {
  auto *map_task_descs = new HeapMmapPopulateTask[req.num_heaps];
  for (uint32_t i = 0; i < req.num_heaps; i++) {
    std::construct_at(&map_task_descs[i], req.ranges[i]);
  }
  Runtime::stack_manager->add_ref_cnt(req.stack_cluster, req.num_heaps);
  mmap_populate_heaps(map_task_descs, req.num_heaps, req.src_ip);
  auto resp = std::make_unique_for_overwrite<RPCRespMap>();
  resp->map_task_descs = map_task_descs;
  return resp;
}

void Migrator::handle_unmap(const RPCReqUnmap &req) {
  delete[] req.map_task_descs;
  Runtime::stack_manager->add_ref_cnt(req.stack_cluster, -req.num_heaps);
  for (uint64_t i = 0; i < req.num_heaps; i++) {
    Runtime::heap_manager->deallocate(req.heaps[i]);
  }
}

void Migrator::handle_fetch(const RPCReqFetch &req, RPCReturner *returner) {
  auto span =
      std::span(reinterpret_cast<const std::byte *>(req.start_addr), req.len);
  returner->Return(kOk, span, nullptr);
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
    RPCClient *client, const std::vector<HeapHeader *> &destructed_heaps,
    HeapMmapPopulateTask *map_task_descs) {
  auto req_buf_len =
      sizeof(RPCReqUnmap) + sizeof(HeapHeader *) * destructed_heaps.size();
  auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
  auto *req = reinterpret_cast<RPCReqUnmap *>(req_buf.get());
  std::construct_at(req);
  req->stack_cluster = Runtime::stack_manager->get_range();
  req->num_heaps = destructed_heaps.size();
  req->map_task_descs = map_task_descs;
  for (uint32_t i = 0; i < destructed_heaps.size(); i++) {
    req->heaps[i] = destructed_heaps[i];
  }
  auto req_span = std::span(req_buf.get(), req_buf_len);
  RPCReturnBuffer return_buf;
  BUG_ON(client->Call(req_span, &return_buf) != kOk);
}

HeapMmapPopulateTask *Migrator::pre_mmap(RPCClient *client,
                                         const std::vector<HeapRange> &heaps) {
  auto req_buf_len = sizeof(RPCReqMap) + sizeof(HeapRange) * heaps.size();
  auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
  auto *req = reinterpret_cast<RPCReqMap *>(req_buf.get());
  std::construct_at(req);
  req->src_ip = get_cfg_ip();
  req->stack_cluster = Runtime::stack_manager->get_range();
  req->num_heaps = heaps.size();
  memcpy(req->ranges, heaps.data(), sizeof(HeapRange) * heaps.size());
  auto req_span = std::span(req_buf.get(), req_buf_len);
  RPCReturnBuffer return_buf;
  BUG_ON(client->Call(req_span, &return_buf) != kOk);
  auto &resp = from_span<RPCRespMap>(return_buf.get_buf());
  return resp.map_task_descs;
}

void Migrator::migrate(RPCClient *client, HeapHeader *heap_header,
                       HeapMmapPopulateTask *map_task_desc) {
  RPCReqMigrate req;
  req.src_ip = get_cfg_ip();
  req.stack_cluster = Runtime::stack_manager->get_range();
  req.heap = heap_header;
  req.map_task_desc = map_task_desc;
  RPCReturnBuffer return_buf;
  BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
}

void Migrator::migrate_heaps(Resource pressure, std::vector<HeapRange> heaps) {
  auto optional_dest_addr =
      Runtime::controller_client->get_migration_dest(pressure);
  BUG_ON(!optional_dest_addr);
  auto *client = Runtime::rpc_client_mgr->get_by_ip(optional_dest_addr->ip);
  auto *map_task_descs = pre_mmap(client, heaps);

  std::vector<HeapHeader *> migrated_heaps;
  std::vector<HeapHeader *> destructed_heaps;
  migrated_heaps.reserve(heaps.size());
  for (uint32_t i = 0; i < heaps.size(); i++) {
    auto [heap_header, _] = heaps[i];
    if (unlikely(!mark_migrating_threads(heap_header))) {
      destructed_heaps.push_back(heap_header);
      continue;
    }
    migrated_heaps.push_back(heap_header);
    pause_migrating_threads();
    migrate(client, heap_header, map_task_descs + i);
    SlabAllocator::deregister_slab_by_id(to_u16(heap_header));
    gc_migrated_threads();
  }

  for (auto *heap_header : migrated_heaps) {
    Runtime::heap_manager->deallocate(heap_header);
  }
  unmap_destructed_heaps(client, destructed_heaps, map_task_descs);
}

void Migrator::fetch(RPCClient *client, uint64_t src_addr, uint64_t dest_addr,
                     uint64_t len) {
  RPCReqFetch req;
  req.start_addr = src_addr;
  req.len = len;
  auto args_span = to_span(req);
  auto resp_callback = [dest_addr](ssize_t len, rt::TcpConn *c) {
    BUG_ON(c->ReadFull(reinterpret_cast<void *>(dest_addr), len) < 0);
  };
  BUG_ON(client->Call(args_span, std::move(resp_callback)) != kOk);
}

void Migrator::load_heap(RPCClient *client, HeapMmapPopulateTask *task) {
  [[maybe_unused]] uint64_t t0, t1, t2;
  if constexpr (kEnableLogging) {
    t0 = rdtsc();
  }
  {
    rt::ScopedLock<rt::Mutex> guard(task->mu.get());
    while (unlikely(!rt::access_once(task->mmapped)))
      task->cv->Wait(task->mu.get());
  }
  if constexpr (kEnableLogging) {
    t1 = rdtsc();
  }

  auto *heap_header = task->range.heap_header;
  auto start_addr = reinterpret_cast<uint64_t>(&heap_header->copy_start);
  auto len = task->range.len - offsetof(HeapHeader, copy_start);

  std::vector<rt::Thread> threads;
  for (uint32_t i = 0; i < kLoadHeapNumThreads; i++) {
    threads.emplace_back([&, tid = i] {
      auto per_thread_len = (len - 1) / kLoadHeapNumThreads + 1;
      auto fetch_src_addr = start_addr + tid * per_thread_len;
      auto fetch_dest_addr = fetch_src_addr;
      auto fetch_len = (tid != kLoadHeapNumThreads - 1)
                           ? per_thread_len
                           : start_addr + len - fetch_src_addr;
      fetch(client, fetch_src_addr, fetch_dest_addr, fetch_len);
    });
  }

  for (auto &thread : threads) {
    thread.Join();
  }

  auto &slab = heap_header->slab;
  nu::SlabAllocator::register_slab_by_id(&slab, slab.get_id());

  if constexpr (kEnableLogging) {
    t2 = rdtsc();
    preempt_disable();
    std::cout << "Load heap: mmap cycles = " << t1 - t0
              << ", tcp cycles = " << t2 - t1 << std::endl;
    preempt_enable();
  }
}

thread_t *Migrator::load_one_thread(RPCClient *client,
                                    const ThreadInfo &thread_info,
                                    HeapHeader *heap_header) {
  auto tf =
      std::make_unique_for_overwrite<std::byte[]>(thread_info.trap_frame_size);
  auto tf_src_addr = reinterpret_cast<uint64_t>(thread_info.trap_frame);
  auto tf_dest_addr = reinterpret_cast<uint64_t>(tf.get());
  auto tf_len = thread_info.trap_frame_size;
  fetch(client, tf_src_addr, tf_dest_addr, tf_len);

  auto stack_src_addr = thread_info.stack_start;
  auto stack_dest_addr = stack_src_addr;
  auto stack_len = thread_info.stack_len;
  fetch(client, stack_src_addr, stack_dest_addr, stack_len);

  auto tlsvar = reinterpret_cast<uint64_t>(&heap_header->slab);
  auto th = create_migrated_thread(tf.get(), tlsvar);
  heap_header->threads->put(th);

  return th;
}

void Migrator::load_mutexes(RPCClient *client, HeapHeader *heap_header,
                            std::vector<thread_t *> *blocked_threads) {
  RPCReqLoadMutexesInfo req;
  req.heap_header = heap_header;
  RPCReturnBuffer return_buf;
  BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  auto span = return_buf.get_buf();
  auto &resp = from_span<RPCRespLoadMutexesInfo>(span);
  auto num_mutexes = span.size() / sizeof(MutexInfo);

  for (uint32_t i = 0; i < num_mutexes; i++) {
    auto *mutex = resp.mutex_infos[i].mutex;
    heap_header->mutexes->put(mutex);

    if (resp.mutex_infos[i].has_blocked_threads) {
      RPCReqLoadMutexThreadInfo req;
      req.mutex = mutex;
      RPCReturnBuffer return_buf;
      BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
      auto span = return_buf.get_buf();
      auto &resp = from_span<RPCRespLoadMutexThreadInfo>(span);
      auto num_threads = span.size() / sizeof(ThreadInfo);
      heap_header->forward_wg.Add(num_threads);

      auto *waiters = mutex->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto &thread_info = resp.thread_infos[j];
        blocked_threads->push_back(thread_info.th);
        auto *th = load_one_thread(client, resp.thread_infos[j], heap_header);
        auto *th_link = reinterpret_cast<list_node *>(
            reinterpret_cast<uintptr_t>(th) + thread_link_offset);
        list_add_tail(waiters, th_link);
      }
    }
  }
}

void Migrator::load_condvars(RPCClient *client, HeapHeader *heap_header,
                             std::vector<thread_t *> *blocked_threads) {
  RPCReqLoadCondvarsInfo req;
  req.heap_header = heap_header;
  RPCReturnBuffer return_buf;
  BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  auto span = return_buf.get_buf();
  auto &resp = from_span<RPCRespLoadCondvarsInfo>(span);
  auto num_condvars = span.size() / sizeof(CondvarInfo);

  for (uint32_t i = 0; i < num_condvars; i++) {
    auto *condvar = resp.condvar_infos[i].condvar;
    heap_header->condvars->put(condvar);

    if (resp.condvar_infos[i].has_blocked_threads) {
      RPCReqLoadCondvarThreadInfo req;
      req.condvar = condvar;
      RPCReturnBuffer return_buf;
      BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
      auto span = return_buf.get_buf();
      auto &resp = from_span<RPCRespLoadCondvarThreadInfo>(span);
      auto num_threads = span.size() / sizeof(ThreadInfo);
      heap_header->forward_wg.Add(num_threads);

      auto *waiters = condvar->get_waiters();
      list_head_init(waiters);
      for (size_t j = 0; j < num_threads; j++) {
        auto &thread_info = resp.thread_infos[j];
        blocked_threads->push_back(thread_info.th);
        auto *th = load_one_thread(client, resp.thread_infos[j], heap_header);
        auto *th_link = reinterpret_cast<list_node *>(
            reinterpret_cast<uintptr_t>(th) + thread_link_offset);
        list_add_tail(waiters, th_link);
      }
    }
  }
}

void Migrator::load_time(RPCClient *client, HeapHeader *heap_header,
                         std::vector<thread_t *> *blocked_threads) {
  RPCReqLoadTimeInfo req;
  req.heap_header = heap_header;
  RPCReturnBuffer return_buf;
  BUG_ON(client->Call(to_span(req), &return_buf) != kOk);
  auto span = return_buf.get_buf();
  auto &resp = from_span<RPCRespLoadTimeInfo>(span);

  auto *time = heap_header->time.get();
  heap_header->forward_wg.Add(resp.num_entries);
  auto loader_tsc = rdtscp(nullptr) - start_tsc;
  time->offset_tsc_ = resp.sum_tsc - loader_tsc;

  for (size_t i = 0; i < resp.num_entries; i++) {
    auto &timer_entry_info = resp.timer_entry_infos[i];
    auto *entry = timer_entry_info.entry;
    auto *arg = reinterpret_cast<TimerCallbackArg *>(entry->arg);
    auto &thread_info = timer_entry_info.thread_info;
    blocked_threads->push_back(thread_info.th);
    auto *th = load_one_thread(client, thread_info, heap_header);
    arg->th = th;
    {
      rt::ScopedLock<rt::Spin> guard(&time->spin_);
      time->entries_.push_back(entry);
      arg->iter = --time->entries_.end();
    }
    entry->armed = false;
    timer_start(entry, time->to_physical_us(arg->logical_deadline_us));
  }
}

void Migrator::load_unblocked_threads(
    RPCClient *client, HeapHeader *heap_header,
    const std::vector<thread_t *> blocked_threads) {
  auto req_buf_len = sizeof(RPCReqLoadUnblockedThreads) +
                     sizeof(thread_t *) * blocked_threads.size();
  auto req_buf = std::make_unique_for_overwrite<std::byte[]>(req_buf_len);
  auto *req = reinterpret_cast<RPCReqLoadUnblockedThreads *>(req_buf.get());
  std::construct_at(req);
  req->heap_header = heap_header;
  req->num_blocked_threads = blocked_threads.size();
  for (uint32_t i = 0; i < req->num_blocked_threads; i++) {
    req->threads[i] = blocked_threads[i];
  }

  RPCReturnBuffer return_buf;
  auto args_span = std::span(req_buf.get(), req_buf_len);
  BUG_ON(client->Call(args_span, &return_buf) != kOk);

  auto resp_span = return_buf.get_buf();
  auto &resp = from_span<RPCRespLoadUnblockedThreads>(resp_span);
  auto num_threads = resp_span.size() / sizeof(ThreadInfo);
  heap_header->forward_wg.Add(num_threads);

  for (uint64_t i = 0; i < num_threads; i++) {
    auto *th = load_one_thread(client, resp.thread_infos[i], heap_header);
    thread_ready(th);
  }
}

void Migrator::mmap_populate_heaps(HeapMmapPopulateTask *map_task_descs,
                                   uint32_t num_descs, uint32_t old_server_ip) {
  rt::Thread([map_task_descs, num_descs, old_server_ip] {
    for (uint32_t i = 0; i < num_descs; i++) {
      auto &task = map_task_descs[i];
      Runtime::heap_manager->mmap_populate(task.range.heap_header,
                                           task.range.len);
      Runtime::heap_manager->setup(task.range.heap_header,
                                   /* migratable = */ false,
                                   /* from_migration = */ true);
      task.range.heap_header->old_server_ip = old_server_ip;
      rt::ScopedLock<rt::Mutex> guard(task.mu.get());
      task.mmapped = true;
      task.cv->SignalAll();
    }
  }).Detach();
}

void Migrator::handle_reserve_conn(const RPCReqReserveConn &req) {
  Runtime::rpc_client_mgr->get_by_ip(req.dest_server_addr.ip);
}

ThreadInfo Migrator::acquire_thread_info(thread_t *th) {
  ThreadInfo thread_info;
  thread_info.th = th;
  thread_info.trap_frame =
      thread_get_trap_frame(th, &thread_info.trap_frame_size);
  auto stack_range = get_obj_stack_range(th);
  thread_info.stack_start = stack_range.start;
  thread_info.stack_len = stack_range.end - stack_range.start;
  thread_mark_migrated(th);
  return thread_info;
}

} // namespace nu

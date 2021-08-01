#pragma once

#include <cstdint>
#include <memory>
#include <functional>

extern "C" {
#include <runtime/net.h>
}

#include "nu/commons.hpp"
#include "nu/utils/archive_pool.hpp"
#include "nu/utils/future.hpp"
#include "nu/utils/rcu_lock.hpp"
#include "nu/utils/slab.hpp"

namespace nu {

struct HeapHeader;
class ObjServer;
class HeapManager;
class StackManager;
class ControllerClient;
class RemObjConnManager;
class Migrator;
class Monitor;
template <typename T> class RuntimeAllocator;
template <typename T> class RuntimeDeleter;

template <typename T>
using RuntimeFuture = Future<T, RuntimeDeleter<Promise<T>>>;

class Runtime {
public:
  enum Mode { CLIENT, SERVER, CONTROLLER };

  static SlabAllocator runtime_slab;

  ~Runtime();
  static std::unique_ptr<Runtime> init(uint32_t remote_ctrl_ip, Mode mode);
  static void reserve_ctrl_server_conns(uint32_t num);
  static void reserve_obj_server_conns(uint32_t num, netaddr obj_server_addr);
  static void reserve_migration_conns(uint32_t num, netaddr dest_server_addr);

private:
  static RCULock rcu_lock;
  static std::unique_ptr<ObjServer> obj_server;
  static std::unique_ptr<ControllerClient> controller_client;
  static std::unique_ptr<HeapManager> heap_manager;
  static std::unique_ptr<StackManager> stack_manager;
  static std::unique_ptr<RemObjConnManager> rem_obj_conn_mgr;
  static std::unique_ptr<Migrator> migrator;
  static std::unique_ptr<Monitor> monitor;
  static std::unique_ptr<ArchivePool<RuntimeAllocator<uint8_t>>> archive_pool;

  friend class Test;
  friend class ObjServer;
  friend class ControllerClient;
  friend class RemObjConnManager;
  friend class Monitor;
  friend class Migrator;
  friend class Mutex;
  friend class CondVar;
  friend class Time;
  friend class DistributedMemPool;
  friend class HeapManager;
  template <typename T> friend class RemObj;
  template <typename T> friend class RemRawPtr;
  template <typename T> friend class RemUniquePtr;
  template <typename T> friend class RemSharedPtr;
  template <typename T> friend class RuntimeDeleter;

  Runtime(uint32_t remote_ctrl_ip, Mode mode);
  static void init_runtime_heap();
  static void init_as_controller();
  static void init_as_server(uint32_t remote_ctrl_ip);
  static void init_as_client(uint32_t remote_ctrl_ip);
  template <typename Cls, typename Fn, typename... As>
  static bool run_within_obj_env(void *heap_base, Fn fn, As &&... args);
  template <typename Cls, typename Fn, typename... As>
  static void __run_within_obj_env(HeapHeader *heap_header, uint8_t *obj_stack,
                                   Cls *obj_ptr, Fn fn, As &&... args);
  static void *get_heap();
  static void set_heap(void *heap);
  static void switch_to_obj_heap(void *obj_ptr);
  static void switch_to_runtime_heap();
  template <typename T, typename... Args>
  static T *new_on_runtime_heap(Args &&... args);
  template <typename T> static void delete_on_runtime_heap(T *ptr);
  static HeapHeader *get_current_obj_heap_header();
  static RemObjID get_current_obj_id();
  template <typename T> static T *get_current_obj();
  template <typename T> static T *get_obj(RemObjID id);
};

int runtime_main_init(int argc, char **argv,
                      std::function<void(int argc, char **argv)> main_func);

} // namespace nu

#include "nu/impl/runtime.ipp"

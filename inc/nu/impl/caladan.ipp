namespace nu {

inline Caladan::PreemptGuard::PreemptGuard() { preempt_disable(); }

inline Caladan::PreemptGuard::~PreemptGuard() { preempt_enable(); }

inline uint32_t Caladan::PreemptGuard::read_cpu() const { return ::read_cpu(); }

template <typename F>
inline void Caladan::PreemptGuard::enable_for(F &&f) {
  preempt_enable();
  f();
  preempt_disable();
}

template <typename F>
inline void Caladan::context_switch_to(F &&f) {
  rt::Preempt p;
  p.Lock();
  rt::Thread(
      [old_th = thread_self(), f = std::forward<F>(f)]() mutable {
        ::thread_wait_until_parked(old_th);
        f();
        ::thread_ready_head(old_th);
      },
      /* head = */ true)
      .Detach();
  p.UnlockAndPark();
}

inline void Caladan::thread_park() {
  rt::Preempt p;
  rt::PreemptGuardAndPark gp(&p);
}

template <typename T>
inline void Caladan::thread_park_and_unlock_np(ScopedLock<T> &&lock) {
  ::thread_park_and_unlock_np(&lock.l_->spinlock_);
  lock.l_ = nullptr;
}

inline void Caladan::thread_park_and_unlock_np(spinlock_t *spin,
                                               list_head *waiters) {
  auto *myth = Caladan::thread_self();
  auto *myth_link = reinterpret_cast<list_node *>(
      reinterpret_cast<uintptr_t>(myth) + thread_link_offset);
  list_add_tail(waiters, myth_link);
  ::thread_park_and_unlock_np(spin);
}

inline void Caladan::thread_exit() { rt::Exit(); }

inline void Caladan::thread_yield() { rt::Yield(); }

inline void Caladan::thread_yield(const PreemptGuard &g) {
  thread_yield_and_preempt_enable();
  preempt_disable();
}

template <typename F>
inline void Caladan::thread_spawn(F &&f) {
  rt::Spawn(std::forward<F>(f));
}

inline thread_t *Caladan::thread_self() { return ::thread_self(); }

inline void Caladan::thread_ready(thread_t *th) { ::thread_ready(th); }

inline void Caladan::thread_ready_head(thread_t *th) {
  ::thread_ready_head(th);
}

inline bool Caladan::preempt_enabled() { return ::preempt_enabled(); }

inline void *Caladan::thread_get_nu_state(thread_t *th, size_t *nu_state_size) {
  return ::thread_get_nu_state(th, nu_state_size);
}

inline thread_t *Caladan::restore_thread(void *nu_state) {
  return ::restore_thread(nu_state);
}

inline ProcletHeader *Caladan::thread_unset_owner_proclet(thread_t *th,
                                                          bool update_monitor) {
  return reinterpret_cast<ProcletHeader *>(
      ::thread_unset_owner_proclet(th, update_monitor));
}

inline ProcletHeader *Caladan::thread_set_owner_proclet(
    thread_t *th, ProcletHeader *owner_proclet, bool update_monitor) {
  return reinterpret_cast<ProcletHeader *>(
      ::thread_set_owner_proclet(th, owner_proclet, update_monitor));
}

inline ProcletHeader *Caladan::thread_get_owner_proclet() {
  return reinterpret_cast<ProcletHeader *>(::thread_get_owner_proclet());
}

inline SlabAllocator *Caladan::thread_get_proclet_slab(void) {
  return reinterpret_cast<SlabAllocator *>(::thread_get_proclet_slab());
}

inline SlabAllocator *Caladan::thread_set_proclet_slab(
    SlabAllocator *proclet_slab) {
  return reinterpret_cast<SlabAllocator *>(
      ::thread_set_proclet_slab(proclet_slab));
}

inline thread_t *Caladan::thread_create_with_buf(thread_fn_t fn, void **buf,
                                                 size_t len) {
  return ::thread_create_with_buf(fn, buf, len);
}

inline thread_t *Caladan::thread_nu_create_with_buf(void *proclet_stack,
                                                    uint32_t proclet_stack_size,
                                                    thread_fn_t fn, void **buf,
                                                    size_t buf_len) {
  return ::thread_nu_create_with_buf(proclet_stack, proclet_stack_size, fn, buf,
                                     buf_len);
}

inline thread_id_t Caladan::get_thread_id(thread_t *th) {
  return ::get_thread_id(th);
}

inline thread_id_t Caladan::get_current_thread_id() {
  return ::get_current_thread_id();
}

template <typename T>
inline T volatile &Caladan::access_once(T &t) {
  return rt::access_once(t);
}

inline void *Caladan::thread_get_runtime_stack_base() {
  return ::thread_get_runtime_stack_base();
}

inline uint64_t Caladan::thread_get_rsp(thread_t *th) {
  return ::thread_get_rsp(th);
}

inline bool Caladan::thread_has_been_migrated() {
  return ::thread_has_been_migrated();
}

inline int32_t Caladan::thread_hold_rcu(RCULock *rcu, bool flag) {
  return ::thread_hold_rcu(rcu, flag);
}

inline int32_t Caladan::thread_unhold_rcu(RCULock *rcu, bool *flag) {
  return ::thread_unhold_rcu(rcu, flag);
}

inline bool Caladan::thread_is_rcu_held(thread_t *th, RCULock *rcu) {
  return ::thread_is_rcu_held(th, rcu);
}

inline void Caladan::thread_start_monitor_cycles() {
  ::thread_start_monitor_cycles();
}

inline void Caladan::thread_end_monitor_cycles() {
  ::thread_end_monitor_cycles();
}

inline bool Caladan::thread_monitored() { return ::thread_monitored(); }

inline void Caladan::thread_flush_all_monitor_cycles() {
  ::thread_flush_all_monitor_cycles();
}

inline void Caladan::unblock_and_relax() {
  pause_local_migrating_threads();
  prioritize_local_rcu_readers();
  cpu_relax();
}

inline void Caladan::wake_one_thread(list_head *waiters) {
  auto *th = reinterpret_cast<thread_t *>(
      const_cast<void *>(list_pop_(waiters, thread_link_offset)));
  if (th) {
    thread_ready(th);
  }
}

inline void Caladan::wake_all_threads(list_head *waiters) {
  while (auto *waketh = reinterpret_cast<thread_t *>(
             const_cast<void *>(list_pop_(waiters, thread_link_offset)))) {
    thread_ready(waketh);
  }
}

inline void Caladan::spin_lock_init(spinlock_t *spin) {
  ::spin_lock_init(spin);
}

inline void Caladan::spin_lock_np(spinlock_t *spin) { ::spin_lock_np(spin); }

inline bool Caladan::spin_try_lock_np(spinlock_t *spin) {
  return ::spin_try_lock_np(spin);
}

inline void Caladan::spin_unlock_np(spinlock_t *spin) {
  ::spin_unlock_np(spin);
}

inline bool Caladan::spin_lock_held(spinlock_t *spin) {
  return ::spin_lock_held(spin);
}

inline void Caladan::mutex_init(mutex_t *mutex) {
  ::mutex_init(mutex);
}

inline bool Caladan::mutex_held(mutex_t *mutex) {
  return ::mutex_held(mutex);
}

inline bool Caladan::mutex_try_lock(mutex_t *mutex) {
  return ::mutex_try_lock(mutex);
}

inline void Caladan::condvar_init(condvar_t *condvar) {
  return ::condvar_init(condvar);
}

inline uint64_t Caladan::rdtsc() { return ::rdtsc(); }

inline uint64_t Caladan::microtime() { return ::microtime(); }

inline void Caladan::timer_init(timer_entry *e, timer_fn_t fn,
                                unsigned long arg) {
  ::timer_init(e, fn, arg);
}

inline void Caladan::timer_start(timer_entry *e, uint64_t deadline_us) {
  ::timer_start(e, deadline_us);
}

inline void Caladan::timer_sleep_until(uint64_t deadline_us) {
  ::timer_sleep_until(deadline_us);
}

inline void Caladan::timer_sleep(uint64_t duration_us) {
  ::timer_sleep(duration_us);
}

}  // namespace nu


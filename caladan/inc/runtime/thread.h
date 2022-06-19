/*
 * thread.h - support for user-level threads
 */

#pragma once

#include <asm/cpu.h>
#include <base/compiler.h>
#include <base/types.h>
#include <iokernel/control.h>
#include <runtime/preempt.h>
#include <stdint.h>

struct thread;
typedef void (*thread_fn_t)(void *arg);
typedef struct thread thread_t;
typedef uint64_t thread_id_t;

struct aligned_cycles {
        uint64_t c;
} __aligned(CACHE_LINE_SIZE);

extern const int thread_link_offset;
extern const int thread_run_cycles_offset;
extern const int thread_owner_proclet_offset;
extern const int thread_proclet_slab_offset;

/*
 * Low-level routines, these are helpful for bindings and synchronization
 * primitives.
 */

extern void thread_park_and_unlock_np(spinlock_t *l);
extern void thread_park_and_preempt_enable(void);
extern void thread_ready(thread_t *thread);
extern void thread_ready_head(thread_t *thread);
extern thread_t *thread_create(thread_fn_t fn, void *arg);
extern thread_t *thread_create_with_buf(thread_fn_t fn, void **buf, size_t len);
extern thread_t *thread_nu_create_with_buf(void *nu_thread, void *proclet_stack,
                                           uint32_t proclet_stack_size,
                                           thread_fn_t fn, void **buf,
                                           size_t buf_len);

extern __thread thread_t *__self;
extern __thread unsigned int kthread_idx;

static inline unsigned int get_current_affinity(void)
{
	return kthread_idx;
}

/**
 * thread_self - gets the currently running thread
 */
inline thread_t *thread_self(void)
{
	return __self;
}

/**
 * get_current_thread_id - get the thread id of the currently running thread
 */
static inline thread_id_t get_current_thread_id(void)
{
	return (thread_id_t)thread_self();
}

static inline thread_id_t get_thread_id(thread_t *th)
{
	return (thread_id_t)th;
}

extern uint64_t get_uthread_specific(void);
extern void set_uthread_specific(uint64_t val);

extern struct aligned_cycles *
thread_start_monitor_cycles(struct aligned_cycles *output);
extern void thread_end_monitor_cycles(struct aligned_cycles *old_output);
extern void thread_flush_all_monitor_cycles(void);
extern struct aligned_cycles *thread_get_monitor_cycles(thread_t *th);
inline bool thread_monitored(void) {
	return *(void **)((uint64_t)__self + thread_run_cycles_offset);
}
extern void thread_hold_rcu(void *rcu);
extern void thread_unhold_rcu(void *rcu);

/*
 * High-level routines, use this API most of the time.
 */

extern void thread_yield(void);
extern int thread_spawn(thread_fn_t fn, void *arg);
extern void thread_exit(void) __noreturn;

/*
 * Used by Nu.
 */
extern bool thread_has_been_migrated(void);
extern bool thread_is_at_creator(void);
extern uint64_t thread_get_rsp(thread_t *th);
extern void pause_migrating_ths_main(void *owner_proclet);
extern void pause_migrating_ths_aux(void);
extern struct list_head all_migrating_ths;
extern void pause_local_migrating_threads(void);
extern void prioritize_rcu_readers(void *rcu);
extern void prioritize_local_rcu_readers(void);
extern void *thread_get_nu_state(thread_t *th, size_t *nu_state_size);
extern thread_t *create_migrated_thread(void *nu_state);
extern void gc_migrated_threads(void);
extern void *thread_get_runtime_stack_base(void);
extern void thread_set_nu_thread(thread_t *th, void *nu_thread);
extern void *thread_get_nu_thread(thread_t *th);
extern uint32_t thread_get_creator_ip(void);
extern void thread_wait_until_parked(thread_t *th);
extern void prealloc_threads_and_stacks(uint32_t num_mags);

static inline void *thread_unset_owner_proclet(void)
{
	void **owner_proclet_p =
		(void **)((uint64_t)__self + thread_owner_proclet_offset);
	void *old_owner_proclet = *owner_proclet_p;
	*owner_proclet_p = NULL;

	return old_owner_proclet;
}

static inline void *thread_set_owner_proclet(thread_t *th, void *owner_proclet)
{
	void **owner_proclet_p =
		(void **)((uint64_t)th + thread_owner_proclet_offset);
	void *old_owner_proclet = *owner_proclet_p;
	*owner_proclet_p = owner_proclet;

	return old_owner_proclet;
}

static inline void *thread_get_owner_proclet(void)
{
	void **owner_proclet_p =
		(void **)((uint64_t)__self + thread_owner_proclet_offset);
	return *owner_proclet_p;
}

static inline void *thread_get_proclet_slab(void)
{
       if (!__self)
		return 0;

       return *(void **)((uint64_t)__self + thread_proclet_slab_offset);
}

static inline void *thread_set_proclet_slab(void *proclet_slab)
{
       void **proclet_slab_p = (void **)((uint64_t)__self + thread_proclet_slab_offset);
       void *old_proclet_slab = *proclet_slab_p;
       *proclet_slab_p = proclet_slab;

       return old_proclet_slab;
}



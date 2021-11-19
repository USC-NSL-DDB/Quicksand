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
static inline thread_id_t get_current_thread_id()  
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
extern bool thread_hold_rcu(void *rcu);
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
extern void thread_mark_migrating(thread_t *thread);
extern void thread_mark_migrated(thread_t *thread);
extern bool thread_is_migrated(void);
extern uint64_t thread_get_rsp(thread_t *th);
extern void pause_all_migrating_threads(void);
extern void pause_local_migrating_threads(void);
extern void *thread_get_nu_state(thread_t *th, size_t *nu_state_size);
extern thread_t *create_migrated_thread(void *nu_state);
extern void gc_migrated_threads(void);
extern void *thread_get_runtime_stack_base(void);
extern void *thread_get_obj_heap(void);
extern void thread_set_obj_heap(void *obj_heap);
extern uint64_t thread_get_waiter_info(thread_t *th);
extern uint64_t thread_get_self_waiter_info(void);
extern void thread_get_waiter_info_and_ready(thread_t *th, uint64_t *waiter_info,
                                             bool *ready);
extern void thread_set_waiter_info(thread_t *th, uint64_t waiter_info);
extern void thread_set_self_waiter_info(uint64_t waiter_info);
extern void thread_set_nu_thread(thread_t *th, void *nu_thread);
extern void *thread_get_nu_thread(thread_t *th);

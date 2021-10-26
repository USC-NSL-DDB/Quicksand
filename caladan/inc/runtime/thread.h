/*
 * thread.h - support for user-level threads
 */

#pragma once

#include <base/compiler.h>
#include <base/types.h>
#include <iokernel/control.h>
#include <runtime/preempt.h>
#include <stdint.h>

struct thread;
typedef void (*thread_fn_t)(void *arg);
typedef struct thread thread_t;
typedef uint64_t thread_id_t;

extern const int thread_link_offset;

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

/*
 * High-level routines, use this API most of the time.
 */

extern void thread_yield(void);
extern int thread_spawn(thread_fn_t fn, void *arg);
extern void thread_exit(void) __noreturn;

extern void thread_mark_migrating(thread_t *thread);
extern void thread_mark_migrated(thread_t *thread);
extern bool thread_is_migrated(void);
extern uint64_t thread_get_rsp(thread_t *th);
extern void *thread_get_trap_frame(thread_t *th, size_t *size);
extern void pause_migrating_threads(void);
extern thread_t *create_migrated_thread(void *tf, void *obj_heap);
extern void gc_migrated_threads(void);
extern void *thread_get_runtime_stack_base(void);
extern void *get_obj_heap(void);
extern void set_obj_heap(void *obj_heap);
extern uint64_t get_waiter_info(thread_t *th);
extern uint64_t get_self_waiter_info(void);
extern void get_waiter_info_and_ready(thread_t *th, uint64_t *waiter_info,
                                      bool *ready);
extern void set_waiter_info(thread_t *th, uint64_t waiter_info);
extern void set_self_waiter_info(uint64_t waiter_info);

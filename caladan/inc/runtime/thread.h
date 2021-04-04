/*
 * thread.h - support for user-level threads
 */

#pragma once

#include <base/types.h>
#include <base/compiler.h>
#include <runtime/preempt.h>
#include <iokernel/control.h>

struct thread;
typedef void (*thread_fn_t)(void *arg);
typedef struct thread thread_t;

extern const int thread_link_offset;

/*
 * Low-level routines, these are helpful for bindings and synchronization
 * primitives.
 */

extern void thread_park_and_unlock_np(spinlock_t *l);
extern void thread_ready(thread_t *thread);
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
extern void thread_set_obj_stack(void *stack_base);
extern void thread_unset_obj_stack(void);
extern void thread_get_obj_stack(thread_t *th, void **base, void **top);
extern void *thread_get_trap_frame(thread_t *th, size_t *size);
extern void pause_migrating_threads(void);
extern thread_t *create_migrated_thread(void *tf, uint64_t tlsvar);
extern void gc_migrated_threads(void);
extern uint64_t thread_get_runtime_stack_base(void);

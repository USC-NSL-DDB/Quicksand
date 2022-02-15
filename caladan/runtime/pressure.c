#include <runtime/pressure.h>

#include "defs.h"

/* real-time resource pressure signals (shared with the iokernel) */
struct resource_pressure_info *resource_pressure_info;
/* number of resource pressure handlers */
uint8_t *num_resource_pressure_handlers;
/* the pressure handlers */
struct thread **resource_pressure_handlers;

struct handler_thread_args {
	resource_pressure_handler handler;
	void *handler_args;
};

static void thread_wrapper(void *args)
{
	struct handler_thread_args *thread_args =
		(struct handler_thread_args *)args;

	while (true) {
		preempt_disable();
		thread_args->handler(thread_args->handler_args);
		thread_park_and_preempt_enable();
	}
}

void add_resource_pressure_handler(resource_pressure_handler handler,
                                   void *args)
{
	struct handler_thread_args *thread_args;

	resource_pressure_handlers[(*num_resource_pressure_handlers)++] =
		thread_create_with_buf(thread_wrapper, (void **)&thread_args,
				       sizeof(struct handler_thread_args));
	thread_args->handler = handler;
	thread_args->handler_args = args;
}

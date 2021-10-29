#include <runtime/pressure.h>

#include "defs.h"

/* real-time resource pressure signals (shared with the iokernel) */
struct resource_pressure_info *resource_pressure_info;
/* the handler for resource pressure */
resource_pressure_handler handler;

void register_resource_pressure_handler(resource_pressure_handler h)
{
	handler = h;
}

void deregister_resource_pressure_handler(void)
{
	handler = NULL;
}

static void invoke_resource_pressure_handler(void *unused)
{
	while (true) {
		handler();
		preempt_disable();
		thread_park_and_preempt_enable();
	}
}

bool check_resource_pressure(void)
{
	struct kthread *k = myk();

	assert_preempt_disabled();
	assert_spin_lock_held(&k->lock);

	if (handler && resource_pressure_info &&
	    resource_pressure_info->status == PENDING)
		if (__sync_bool_compare_and_swap(
			 &resource_pressure_info->status, PENDING, HANDLING)) {
			thread_ready_head_locked(k->resource_pressure_handler);
			return true;
		}
	return false;
}

int resource_pressure_init_thread(void)
{
	struct kthread *k = myk();
	thread_t *th;

	th = thread_create(invoke_resource_pressure_handler, k);
	if (!th)
		return -ENOMEM;

	k->resource_pressure_handler = th;
	return 0;
}

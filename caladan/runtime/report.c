#include <asm/ops.h>
#include <runtime/report.h>

struct resource_reporting *resource_reporting;

void set_resource_reporting_handler(thread_t *handler) {
	resource_reporting->handler = handler;
}

void finish_resource_reporting(bool forced) {
	if (forced)
		resource_reporting->status = HANDLED;
	resource_reporting->last_tsc = rdtsc();
}

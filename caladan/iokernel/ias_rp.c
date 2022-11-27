#include <stdint.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"
#include "sched.h"

static void ias_rp_preempt_core(struct ias_data *sd, uint64_t now_tsc)
{
	unsigned int i, core;
	struct thread *th;

	for (i = 0; i < sd->p->active_thread_count; i++)
		if (!sd->p->active_threads[i]->preemptor->th)
			break;

	if (unlikely(i == sd->p->active_thread_count))
		if (unlikely(ias_add_kthread(sd) != 0))
			return;

	core = sd->p->active_threads[i]->core;
	sd->p->resource_reporting->last_tsc = now_tsc;
	sd->p->resource_reporting->status = HANDLING;
	barrier();
	th = sd->p->active_threads[i];
	th->preemptor->th = sd->p->resource_reporting->handler;
	th->preemptor->ready_tsc = now_tsc;
	barrier();
	sched_yield_on_core(core);
}

void ias_rp_poll(void)
{
	struct ias_data *sd;
	struct resource_reporting *report;
	uint64_t now_tsc = rdtsc();

	ias_for_each_proc(sd) {
		report = sd->p->resource_reporting;
		if (report->status == HANDLED)
			report->status = NONE;

		if (report->status == NONE)
                        if (report->handler &&
                            report->last_tsc +
                            IAS_RP_INTERVAL_US * cycles_per_us < now_tsc)
                                ias_rp_preempt_core(sd, now_tsc);
        }
}

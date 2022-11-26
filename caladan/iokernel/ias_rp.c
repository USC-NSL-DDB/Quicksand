#include <stdint.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"
#include "sched.h"

static inline void ias_rp_repreempt(struct ias_data *sd, uint64_t now_tsc)
{
	sd->p->resource_reporting->last_tsc = now_tsc;
	if (*sd->rp_preempt_th->preemptor)
		sched_preempt(sd->rp_preempt_core, sd->rp_preempt_th);
}

static inline void ias_rp_grant_exclusive_access(struct ias_data *sd,
						 unsigned int core)
{
	if (!bitmap_test(sd->reserved_cores, core)) {
		bitmap_set(sd->reserved_cores, core);
		bitmap_set(sd->reserved_report_handler_cores, core);
	}
}

static inline void ias_rp_ungrant_exclusive_access(struct ias_data *sd)
{
	int pos;

	bitmap_for_each_set(sd->reserved_report_handler_cores, NCPU, pos) {
		bitmap_clear(sd->reserved_report_handler_cores, pos);
		bitmap_clear(sd->reserved_cores, pos);
	}
}

static void ias_rp_preempt_core(struct ias_data *sd, uint64_t now_tsc)
{
	unsigned int i, core;
	struct thread *th;

	for (i = 0; i < sd->p->active_thread_count; i++)
		if (!(*sd->p->active_threads[i]->preemptor))
			break;

	if (unlikely(i == sd->p->active_thread_count))
		if (unlikely(ias_add_kthread(sd) != 0))
			return;

	core = sd->p->active_threads[i]->core;
	ias_rp_grant_exclusive_access(sd, core);
	sd->p->resource_reporting->last_tsc = now_tsc;
	sd->p->resource_reporting->status = HANDLING;
	barrier();
	th = sd->p->active_threads[i];
	*th->preemptor = sd->p->resource_reporting->handler;
	barrier();
	sd->rp_preempt_core = core;
	sd->rp_preempt_th = th;
	sched_preempt(core, th);
}

void ias_rp_poll(void)
{
	struct ias_data *sd;
	struct resource_reporting *report;
	uint64_t now_tsc = rdtsc();

	ias_for_each_proc(sd) {
		report = sd->p->resource_reporting;
		if (report->status == HANDLED) {
			ias_rp_ungrant_exclusive_access(sd);
			report->status = NONE;
		}

		if (report->status == NONE)
                        if (report->handler &&
                            report->last_tsc +
                            IAS_RP_INTERVAL_US * cycles_per_us < now_tsc)
                                ias_rp_preempt_core(sd, now_tsc);

		if (report->status == HANDLING &&
		    report->last_tsc +
		    IAS_PREEMPT_RETRY_US * cycles_per_us < now_tsc)
			ias_rp_repreempt(sd, now_tsc);
        }
}
